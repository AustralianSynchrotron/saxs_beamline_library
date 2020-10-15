import asyncio
import concurrent.futures
import json
import logging
from collections import OrderedDict
from functools import partial, wraps
from pathlib import Path
from time import sleep, time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Union

from fastapi import Depends, FastAPI, WebSocket
from fastapi import status as status_codes
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from ophyd import EpicsMotor, EpicsSignal
from ophyd import __version__ as ophyd_version
from ophyd.ophydobj import OphydObject, UnknownSubscription
from ophyd.signal import (AlarmStatus, ConnectionTimeoutError, EpicsSignalBase,
                          LimitError)
from ophyd.status import StatusBase
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect, WebSocketState

from saxs_beamline_library import __version__
from saxs_beamline_library.utils.utils import InvalidDevice, get_device


class SubscriptionError(Exception):
    def __init__(self, message, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message = message


class ExistingTokenError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class ReverseSubscription(BaseModel):
    tokens: set
    device_obj: OphydObject

    class Config:
        arbitrary_types_allowed = True


class Subscriptions(BaseModel):
    devices: Set[str]
    websockets: List[WebSocket]
    last_accessed: float= 0

    class Config:
        arbitrary_types_allowed = True

def token_accessed(func):
    @wraps(func)
    def wrapper(self, token, *args, **kwargs):
        self._subscriptions[token].last_accessed = time()
        return func(self, token, *args, **kwargs)

    return wrapper


class OphydConnector:
    def __init__(self, loop: asyncio.BaseEventLoop, reconnect_timeout: float = 60):
        super().__init__()
        self._loop = loop
        self._reconnect_timeout = reconnect_timeout
        self._subscriptions: Mapping[Any, Subscriptions] = {}
        self._devices: List[str] = []
        self._device_lock: Set[str] = set()
        self._reverse_subscriptions: Mapping[Any, ReverseSubscription] = {}
        self._token_buffer: Dict[str, set] = {}
        self._loop.create_task(self._clean_subscriptions())

    async def _clean_subscriptions(self):
        while True:
            await asyncio.sleep(self._reconnect_timeout)
            now = time()
            rm_list = []
            for k,v in self._subscriptions.items():
                if v.websockets == []:
                    if now - v.last_accessed > self._reconnect_timeout:
                        rm_list.append(k)
            for tkn in rm_list:
                self.remove_token(tkn)

    @property
    def subscriptions(self):
        return self._subscriptions

    def new_subscription(self, token: Any, devices=Iterable[str]):
        if token in self._subscriptions.keys():
            raise ExistingTokenError

        self._subscriptions[token] = Subscriptions(devices=set(), websockets=[], last_accessed=time())

        self._loop.create_task(self.add_subscription(token, devices))
    
    @token_accessed
    async def add_subscription(self, token: Any, devices=Iterable[str]):

        def subscriber(device):
            device_obj=get_device(device)
            device_obj.subscribe(partial(self._send_update, device))
            return device_obj

        if token not in self._subscriptions.keys():
            raise InvalidTokenError

        for device in devices:

            if device in self._devices:
                self._subscriptions[token].devices.add(device)
                self._reverse_subscriptions[device].tokens.add(token)

            if device in self._device_lock:
                try:
                    self._token_buffer[device].add(token)
                except KeyError:
                    self._token_buffer[device] = {token}

            if device not in self._devices and device not in self._device_lock:
                try:
                    self._device_lock.add(device)
                    print("add device")
                    print(f"{devices}")
                                        
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        device_obj = await self._loop.run_in_executor(
                            pool, partial(subscriber, device)
                        )
                        
                    self._devices.append(device)

                    tokens = [token]            
        
                    try:
                        tokens.extend(self._token_buffer[device])
                    except KeyError:
                        pass

                    for tkn in tokens:
                        self._subscriptions[token].devices.add(device)
                        self._reverse_subscriptions[device] = ReverseSubscription(
                            tokens={tkn}, device_obj=device_obj
                        )
                finally:
                    self._device_lock.remove(device)

            print("added device")
            print(f"{devices}")

    @token_accessed
    def add_websocket(self, token: Any, websocket: WebSocket):
        if websocket not in self._subscriptions[token].websockets:
            self._subscriptions[token].websockets.append(websocket)

    @token_accessed
    def remove_websocket(self, token: Any, websocket: WebSocket):
        self._subscriptions[token].websockets.remove(websocket)

    
    def remove_token(self, token: Any, force: bool = False):
        if not force:
            if self._subscriptions[token].websockets:
                return False

        for device in self._subscriptions[token].devices:
            self._reverse_subscriptions[device].tokens.remove(token)
            if not self._reverse_subscriptions[device].tokens:
                self._reverse_subscriptions[device].device_obj.unsubscribe_all()
                del self._reverse_subscriptions[device]
                self._devices.remove(device)

        for websocket in self._subscriptions[token].websockets:
            if websocket.client_state is not WebSocketState.DISCONNECTED:
                self._loop.create_task(websocket.close())

        try:
            del self._subscriptions[token]
        except KeyError:
            pass

        return True

    def _send_update(self, device, sub_type, value, **kwargs):
        if sub_type == "readback" or sub_type == "value":
            try:
                alarm_status = device.alarm_status
            except AttributeError:
                try:
                    alarm_status = device.user_readback.alarm_status
                except AttributeError:
                    alarm_status = AlarmStatus(0)

            reply = {
                "type": "library_update",
                "data": {
                    "device": device,
                    "value": value,
                    "alarm_status": alarm_status.name,
                },
            }

            try:
                for token in self._reverse_subscriptions[device].tokens:
                    for client in self._subscriptions[token].websockets:
                        if client.client_state == WebSocketState.CONNECTED:
                            self._loop.create_task(client.send_json(reply))
            except KeyError:
                pass


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

base_url = "/api/v1.0"
ophyd_connector = None


def get_ophyd_connector():
    return ophyd_connector


class SetDevice(BaseModel):
    value: Any
    timeout: float = 10


class AddDevice(BaseModel):
    devices: List[str]


class Subscription(BaseModel):
    token: Any
    devices: List[str] = []


@app.on_event("startup")
async def startup_event():
    global ophyd_connector
    ophyd_connector = OphydConnector(asyncio.get_event_loop())


@app.get(f"{base_url}/")
async def root():
    return {"beamline_library_version": __version__, "ophyd_version": ophyd_version}


@app.post(f"{base_url}/subscriptions", summary="Create new subscription to a device")
async def subscribe(
    subscription: Subscription, subscription_store=Depends(get_ophyd_connector),
):
    """Create new subscription to a device.

    Creates a new subscription to a saxs beamline library ophyd device using the module address.
    The subscription is identified by a unique token provided by the client. 

    Args:
        subscription: A Subscription pydantic model which has parameters of:
            token: A unique string provided by the client
            devices: A list of device address to subscribe to.
        subscription_store: Depends to return an instance of the ophyd connector/subscription store.

    Returns:
        A dict or JSONResponse with the request type subscription, success status, and a message in case of error. For example:

        {"type": "subscription", "success": False, "message": "Invalid Device String"

    """

    response = dict(
        {"type": "subscription", "success": None, "message": ""}, **subscription.dict()
    )

    try:
        subscription_store.new_subscription(subscription.token, subscription.devices)
        response["success"] = True
    except ExistingTokenError:
        response["success"] = False
        response["message"] = "Token already exists"

        return JSONResponse(
            status_code=status_codes.HTTP_409_CONFLICT, content=response,
        )

    except TimeoutError:
        response["success"] = False
        response["message"] = "Timeout Connecting to the device"

        return JSONResponse(
            status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR, content=response,
        )
    except InvalidDevice:
        response["success"] = False
        response["message"] = "Invalid Device String"

        return JSONResponse(
            status_code=status_codes.HTTP_404_NOT_FOUND, content=response,
        )

    return response


@app.get(f"{base_url}/subscriptions/", summary="Get dictionary of subscriptions")
async def subscriptions(
    token: Any = 0, device: str = "", subscription_store=Depends(get_ophyd_connector)
):

    """Get a dictionary of current subsctriptions
    
    Returns a dictionary of the current subsctiptions. The key is the token, and the value is a list of devices.
    """

    return {k: v.devices for k, v in subscription_store.subscriptions.items()}


@app.put(f"{base_url}/subscriptions/{{token}}", summary="Add device to subscription")
async def add_device_to_subscription(
    *,
    token: str,
    devices: AddDevice,
    subscription_store: OphydConnector = Depends(get_ophyd_connector),
):
    
    try:
        await subscription_store.add_subscription(token, devices.devices)
        return {"device": devices.devices[0]}
    except InvalidTokenError:
        start = time()
        while time() - start < 5:
            await asyncio.sleep(0.1)
            try:
                await subscription_store.add_subscription(token, devices.devices)
                return {"device": devices.devices[0]}
            except InvalidTokenError:
                continue
        return JSONResponse(status_code=status_codes.HTTP_404_NOT_FOUND)
    except Exception:
        return JSONResponse(status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get(f"{base_url}/subscriptions/{{token}}", summary="Get info about a subscription")
async def enumerate_subscription(
    token: Any = 0, device: str = "", subscription_store=Depends(get_ophyd_connector)
):

    """Get a list of devices and number of websocket connections for a subsctription
    """

    return {k: v.devices for k, v in subscription_store.subscriptions.items()}


@app.put(f"{base_url}/devices/{{device}}")
async def set_device(*, device: str, set_params: SetDevice) -> None:

    value = set_params.value
    timeout = set_params.timeout

    device_split = device.split(".")
    if device_split[0] == "saxs_motors" and device_split[-1] == "_tweak":
        motor = ".".join(device_split[:-1])
        device_cpt = get_device(motor)
        try:
            status = device_cpt._tweak(value)
            start_time = time()
            while not status.done:
                if time() - start_time > timeout:
                    raise TimeoutError
                await asyncio.sleep(1)

            status = {
                "device": motor,
                "set_done": True,
                "set_success": False,
                "set_message": "",
            }

        except LimitError as e:
            status = {
                "device": motor,
                "set_done": True,
                "set_success": False,
                "set_message": str(e),
            }

        except TimeoutError as e:
            status = {
                "device": device,
                "set_done": False,
                "set_success": False,
                "set_message": str(e),
            }
    else:

        device_cpt = get_device(device)

        if hasattr(device_cpt, "_set_thread"):
            if device_cpt._set_thread is not None:
                return
        try:
            status = device_cpt.set(value, timeout=timeout)
            start_time = time()
            while not status.done:
                if time() - start_time > timeout:
                    raise TimeoutError
                await asyncio.sleep(1)

            status = {
                "device": device,
                "set_done": status.done,
                "set_success": status.success,
                "set_message": "",
            }

        except LimitError as e:
            status = {
                "device": device,
                "set_done": True,
                "set_success": False,
                "set_message": str(e),
            }

        except TimeoutError as e:
            status = {
                "device": device,
                "set_done": False,
                "set_success": False,
                "set_message": str(e),
            }

    return status


@app.get(f"{base_url}/devices/{{device}}")
async def get_device(*, device: str, describe: bool = False) -> dict:
    def get_data(device):
        try:
            device_cpt = get_device(device)
        except TimeoutError:
            return JSONResponse(
                status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "type": "get_device",
                    "device": device,
                    "message": "Timeout Connecting to device",
                },
            )
        except InvalidDevice:
            return JSONResponse(
                status_code=status_codes.HTTP_404_NOT_FOUND,
                content={
                    "type": "get_device",
                    "device": device,
                    "message": "Invalid Device String",
                },
            )

        try:
            value = device_cpt.position
        except AttributeError:
            value = device_cpt.get()

        description = {}

        if describe:

            description = device_cpt.describe()

            if isinstance(description, OrderedDict):
                description = description.popitem(last=False)[1]
            else:
                description = description.popitem()[1]

            description["device"] = device
            description["name"] = device_cpt.name

            if isinstance(device_cpt, EpicsMotor):
                description["lower_disp_limit"] = description["lower_ctrl_limit"]
                description["upper_disp_limit"] = description["upper_ctrl_limit"]

            if isinstance(device_cpt, EpicsSignalBase):
                description["lower_disp_limit"] = device_cpt._read_pv.lower_disp_limit
                description["upper_disp_limit"] = device_cpt._read_pv.upper_disp_limit

            if isinstance(device_cpt, EpicsSignal):
                description["write_pv"] = device_cpt._write_pv.pvname
                description["lower_ctrl_limit"] = device_cpt._write_pv.lower_ctrl_limit
                description["upper_ctrl_limit"] = device_cpt._write_pv.upper_ctrl_limit

        data = {"device": device, "value": value}
        data.update(description)
        return data

    loop = asyncio.get_running_loop()

    with concurrent.futures.ThreadPoolExecutor() as pool:
        try:
            data = await loop.run_in_executor(pool, partial(get_data, device))
        except ConnectionTimeoutError:
            return JSONResponse(
                status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "type": "get_device",
                    "device": device,
                    "message": "Timeout Connecting to device",
                },
            )

    return data


@app.get(f"{base_url}/bundles/{{bundle}}")
async def get_bundle_list(*, bundle: str) -> dict:
    bundle_device = get_device(bundle)
    component_names = list(bundle_device.component_names)

    for component in bundle_device.component_names:
        try:
            getattr(bundle_device, component).wait_for_connection()
        except TimeoutError:
            component_names.remove(component)

    return {
        "type": "get_bundle_list",
        "data": {"bundle": bundle, "name": bundle_device.name, "list": component_names},
    }


@app.websocket(f"{base_url}/subscriptions/{{token}}/ws")
async def ophyd_updates(
    websocket: WebSocket, token: Any, subscription_store=Depends(get_ophyd_connector),
):

    await websocket.accept()

    try:
        subscription_store.add_websocket(token, websocket)
    except KeyError:
        await websocket.close(code=4000)
        return

    try:
        while True:
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        subscription_store.remove_websocket(token, websocket)


if __name__ == "__main__":
    import click
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    LOGGING_CONFIG["formatters"]["default"]["fmt"] = "%(asctime)s [%(name)s] %(levelprefix)s %(message)s"

    @click.command()
    @click.option('--log-level', default='info', help='Standard Log levels. "debug", "info", "warning", "error"')
    def run_server(log_level):
        uvicorn.run(
            "ophyd_api:app", host="0.0.0.0", port=80, log_level=log_level, reload=False,
        )

    run_server()
