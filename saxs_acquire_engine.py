import asyncio
import logging
from functools import partial, wraps
from pathlib import Path
from queue import Queue
from threading import Thread
from time import sleep, time
from typing import Any, Callable, Dict, List, NoReturn, Optional

from bluesky import RunEngine, RunEngineInterrupted
from bluesky.plans import count, grid_scan, list_scan, rel_grid_scan, scan
from bluesky.preprocessors import SupplementalData
from ophyd import Device, EpicsMotor
from ophyd.signal import EpicsSignal, EpicsSignalRO
from ruamel.yaml import YAML

from saxs_beamline_library import plans as bl_plans
from saxs_beamline_library.beamline_kafka.bluesky_producer import BlueskyKafkaProducer
from .saxs_acquire_seq import AcquireSeq
from saxs_beamline_library.devices.saxs_detectors import saxs_detector
from saxs_beamline_library.utils.utils import (
    InvalidComponent,
    get_component,
    put_if_different,
)

logger = logging.getLogger(__name__)


def wait_for_response(func):
    @wraps(func)
    def wrapper(self, *args, wait: bool = False, timeout=2, **kwargs):
        r = func(self, *args, **kwargs)
        if r:
            return r

        if wait:
            states_map = {
                "pause": "paused",
                "resume": "running",
                "stop": "stopping",
                "abort": "aborting",
                "halt": "halting",
            }
            try:
                state = states_map[func.__name__]
            except KeyError:
                state = "idle"

            start = time()
            while self.state != state:
                if time() - start > timeout:
                    raise TimeoutError
                sleep(0.1)
            return True

        return r

    return wrapper


class Counter:
    def __init__(self, start: int = 0):
        self._start = start
        self._counter = start - 1

    def current(self) -> int:
        return self._counter

    def next(self) -> int:
        self._counter += self._counter
        return self._counter

    def reset(self, start: int = None):
        if start is not None:
            self._start = start

        self._counter = self._start - 1


class SAXSAcquire:
    def __init__(
        self,
        config: Optional[Path] = None,
        queue_callback: Optional[Callable] = None,
        loop: Optional[asyncio.BaseEventLoop] = None,
        acquire_seq: Optional[AcquireSeq] = None
    ):
        self._acquire_seq = acquire_seq if acquire_seq is not None else AcquireSeq(name="AcquireSeq")

        self._q_callbacks = dict()
        self._q_callback_counter = Counter()
        if queue_callback is not None:
            self._q_callbacks[self._q_callback_counter.next()] = queue_callback

        self._loop = loop or asyncio.get_event_loop()

        self._sd = SupplementalData()
        self.detectors: List[Device] = []

        if config is not None:
            yaml = YAML(typ="safe")
            configdata = yaml.load(Path(config))

            def append_components(component_list: List[str], appender: Callable[[str], None]) -> None:
                for cpt in component_list:
                    try:
                        component = get_component(cpt)
                        appender(component)
                    except TimeoutError:
                        logger.warning(f"Timeout Connecting Component {cpt}")
                        continue
                    except InvalidComponent as e:
                        logger.warning(str(e))

            append_components(configdata["log"]["monitors"], self._sd.monitors.append)
            append_components(configdata["log"]["baseline"], self._sd.baseline.append)
            append_components(configdata["log"]["shot"], self.detectors.append)

        try:
            self.scaler_mode = EpicsSignal("SR13ID01HU02IOC02:scaler1.CONT")
        except TimeoutError as e:
            logger.error(f"{str(e)} acquisition may not proceed correctly")
        try:
            s2 = EpicsSignalRO("SR13ID01HU02IOC02:mca2", name="I0")
        except TimeoutError as e:
            logger.error(f"{str(e)} acquisition may not proceed correctly")
        try:
            s3 = EpicsSignalRO("SR13ID01HU02IOC02:mca3", name="beamstop")
        except TimeoutError as e:
            logger.error(f"{str(e)} acquisition may not proceed correctly")
        try:
            nord = EpicsSignalRO("SR13ID01HU02IOC02:NORD", name="mca nord")
        except TimeoutError as e:
            logger.error(f"{str(e)} acquisition may not proceed correctly")

        self._sd.monitors.append(nord)
        

        self.detectors.extend(
            [
                self._acquire_seq,
                s2,
                s3,
                saxs_detector.full_file_name,
                saxs_detector.cam.acquire_time,
                saxs_detector.cam.num_images,
                saxs_detector.cam.string_from_server,
            ]
        )
        self.exp_path = Path()
        self._q: Queue = Queue()
        self._cb_q = asyncio.Queue()
        self._producer = BlueskyKafkaProducer()
        self._run_uid = None
        self.thread = self.start_RE_thread()
        self._cb_task = asyncio.ensure_future(self._listen_cb_q())

    def start_RE_thread(self) -> Thread:
        def thread_task() -> NoReturn:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._RE = RunEngine(context_managers=[])
            self._RE.subscribe_lossless(self._producer.send)
            self._RE.preprocessors.append(self._sd)

            while True:
                try:
                    task = self._q.get(True)
                    try:
                        task_type = task["type"]
                    except KeyError:
                        logger.exception("Message to RunEngine q in acquire engine doesn't have key 'type'")
                        continue

                    self._loop.call_soon_threadsafe(self._cb_q.put_nowait, {"event": "received", "task": task})

                    if task_type == "plan":
                        if self._RE.state == "idle":
                            if "callbacks" in task.keys():
                                callbacks = task["callbacks"]
                            else:
                                callbacks = None
                            self._RE(task["data"](), callbacks)
                    if task_type == "resume":
                        self._RE.resume()
                    if task_type == "halt":
                        self._RE.halt()
                    elif task_type == "abort":
                        self._RE.abort()
                    elif task_type == "stop":
                        self._RE.stop()
                except RunEngineInterrupted as e:
                    self._loop.call_soon_threadsafe(
                        self._cb_q.put_nowait, {"event": "run interrupted", "task": task},
                    )
                except Exception as e:
                    self._loop.call_soon_threadsafe(
                        self._cb_q.put_nowait, {"event": "exception", "task": task, "message": str(e)},
                    )
                finally:
                    self._loop.call_soon_threadsafe(self._cb_q.put_nowait, {"event": "done", "task": task})
                    self._q.task_done()

        runengine_thread = Thread(target=thread_task, daemon=False)
        runengine_thread.start()
        return runengine_thread

    def add_callback(self, callback: Callable):
        cid = self._q_callback_counter.next()
        self._q_callbacks[cid] = callback
        return cid

    async def _listen_cb_q(self):
        while True:
            payload = await self._cb_q.get()
            await self._run_q_callbacks(payload)

    async def _run_q_callbacks(self, payload):
        task = payload["task"]
        if task["type"] == "plan":
            payload["task"] = f"plan: {task['data'].func.__name__}"
        try:
            for _, callback in self._q_callbacks.items():
                await callback(payload)
        except Exception as e:
            logger.exception("Exception in q callback in saxs acquire engine")
            pass

    @property
    def state(self):
        return self._RE.state

    def wait_for_state(self, wait_for_change: float):
        state = self.state
        start = time()
        while time() - start < wait_for_change:
            if state != self.state:
                sleep(0.05)
                break
        return self.state

    def queue(self, message: Dict[str, Any], wait=False) -> None:
        self._q.put(message)
        if wait:
            self.join()

    def join(self):
        self._q.join()

    def queue_length(self) -> int:
        return self._q.qsize()

    def clear_queue(self) -> None:
        while not self._q.empty():
            self._q.get_nowait()
            self._q.task_done()

    @wait_for_response
    def pause(self) -> None:
        if self.state == "paused":
            return True
        self._RE.request_pause()
        return False

    @wait_for_response
    def resume(self) -> None:
        if self.state == "paused":
            self.queue({"type": "resume"})
        return False

    @wait_for_response
    def stop(self) -> None:
        if self.state == "idle":
            return True
        if self.state != "paused":
            self.pause()
        self.clear_queue()
        self.queue({"type": "stop"})
        return False

    @wait_for_response
    def abort(self) -> None:
        if self.state == "idle":
            return True
        if self.state == "running":
            self.pause()
        self.clear_queue()
        self.queue({"type": "abort"})
        return False

    @wait_for_response
    def halt(self) -> None:
        if self.state == "idle":
            return True
        if self.state == "running":
            self.pause()
        self.clear_queue()
        self.queue({"type": "halt"})
        return False

    ################################################################################

    def set_parameters(
        self,
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        exp_time: Optional[float] = None,
        num_images: int = 1,
        use_shutter: bool = False,
    ) -> None:

        self._acquire_seq.use_shutter = use_shutter

        self.scaler_mode.put(0)
        sleep_count = 0

        # 0.1 second delay after setting any detector params. I think this is needed for it's happiness.

        if filename is not None:
            if put_if_different(saxs_detector.file_name, filename.as_posix()):
                sleep_count += 1
                sleep(0.1)
        if image_path is not None:
            if put_if_different(saxs_detector.file_path, image_path.as_posix()):
                sleep_count += 1
                sleep(0.1)
        if num_images is not None:
            if put_if_different(saxs_detector.cam.num_images, 1):
                sleep_count += 1
                sleep(0.1)
        if exp_time is not None:
            if put_if_different(saxs_detector.cam.acquire_time, exp_time):
                sleep_count += 1
                sleep(0.1)
            if put_if_different(saxs_detector.cam.acquire_period, exp_time + 0.05):
                sleep_count += 1
                sleep(0.1)

        sleep(max(0.0, 0.5 - sleep_count * 0.1))  # To ensure 0.5 sec delay after setting scaler mode.

    def acquire(
        self,
        filename: Optional[Path] = None,
        exp_times: Optional[List[float]] = None,
        num_images: Optional[int] = 1,
        delay: Optional[int] = 0,
        description: Optional[str] = "",
        use_shutter: bool = False,
        wait: bool = False,
    ) -> None:
        if exp_times is not None:
            if len(exp_times) > 1:
                self.time_scan(
                    exp_times=exp_times,
                    filename=filename,
                    num_images=num_images,
                    delay=delay,
                    use_shutter=use_shutter,
                )
                return
        self.count(
            filename,
            exp_times=exp_times,
            num_images=num_images,
            delay=delay,
            use_shutter=use_shutter,
            wait=wait,
        )

    def count(
        self,
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        exp_times: Optional[List[float]] = None,
        num_images: Optional[int] = 1,
        delay: Optional[int] = 0,
        use_shutter: bool = False,
        wait: bool = False,
    ) -> None:
        self.set_parameters(
            filename=filename,
            image_path=image_path,
            exp_time=None if exp_times is None else exp_times[0],
            num_images=num_images,
            use_shutter=use_shutter,
        )

        self.queue(
            {"type": "plan", "data": partial(count, self.detectors, num_images, delay)}, wait=wait,
        )

    def time_scan(
        self,
        exp_times: List[float],
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        num_images: Optional[int] = 1,
        delay: Optional[float] = 0,
        use_shutter: bool = False,
        wait: bool = False,
    ) -> None:

        self.set_parameters(
            filename=filename,
            image_path=image_path,
            exp_time=min(exp_times),
            num_images=num_images,
            use_shutter=use_shutter,
        )
        self.queue(
            {
                "type": "plan",
                "data": partial(
                    bl_plans.repeat_list_scan,
                    self.detectors,
                    saxs_detector.cam.acquire_time,
                    exp_times,
                    num=num_images,
                    delay=delay,
                ),
            },
            wait=wait,
        )

    # def scan(self,
    #          filename: Optional[Path] = None,
    #          image_path: Optional[Path] = None,
    #          exp_times: Optional[List[float]] = None, num_shots: int = 1,
    #          motor: Optional[Device] = None,
    #          start: Optional[int] = None, stop: Optional[int] = None,
    #          points: Optional[int] = None) -> None:
    #
    #     self.set_parameters(filename=filename, image_path=image_path, exp_times=exp_times, num_shots=num_shots)
    #     self.queue({'type': 'plan', 'data': partial(scan, self.detectors, motor, start, stop, points)})

    def grid(
        self,
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        exp_time: Optional[int] = None,
        num_images: int = 1,
        motor1: Optional[EpicsMotor] = None,
        start1: Optional[int] = None,
        stop1: Optional[int] = None,
        points1: Optional[int] = None,
        motor2: Optional[EpicsMotor] = None,
        start2: Optional[int] = None,
        stop2: Optional[int] = None,
        points2: Optional[int] = None,
        serpentine: bool = True,
        use_shutter: bool = False,
        wait: bool = False,
    ) -> None:

        self.set_parameters(
            filename=filename,
            image_path=image_path,
            exp_time=exp_time,
            num_images=num_images,
            use_shutter=use_shutter,
        )

        self.queue(
            {
                "type": "plan",
                "data": partial(
                    rel_grid_scan,
                    self.detectors,
                    motor1,
                    start1,
                    stop1,
                    points1,
                    motor2,
                    start2,
                    stop2,
                    points2,
                    serpentine,
                ),
            },
            wait=wait,
        )

    def list_1d(
        self,
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        exp_time: Optional[int] = None,
        num_images: int = 1,
        motor1: Optional[EpicsMotor] = None,
        points1: Optional[List[float]] = None,
        filename_device: Optional[EpicsSignal] = None,
        filenames: Optional[List[str]] = None,
        wait: bool = False,
    ) -> None:

        self.set_parameters(
            filename=filename, image_path=image_path, exp_time=exp_time, num_images=num_images,
        )
        self.queue(
            {
                "type": "plan",
                "data": partial(
                    list_scan, self.detectors, motor1, points1, filename_device, filenames,
                ),
            },
            wait=wait,
        )

    def table(
        self,
        filename: Optional[Path] = None,
        image_path: Optional[Path] = None,
        exp_time: Optional[int] = None,
        num_images: int = 1,
        motor1: Optional[EpicsMotor] = None,
        points1: Optional[List[float]] = None,
        motor2: Optional[EpicsMotor] = None,
        points2: Optional[List[float]] = None,
        filename_device: Optional[EpicsSignal] = None,
        filenames: Optional[List[str]] = None,
        wait: bool = False,
    ) -> None:

        self.set_parameters(
            filename=filename, image_path=image_path, exp_time=exp_time, num_images=num_images,
        )
        self.queue(
            {
                "type": "plan",
                "data": partial(
                    list_scan, self.detectors, motor1, points1, motor2, points2, filename_device, filenames,
                ),
            },
            wait=wait,
        )

