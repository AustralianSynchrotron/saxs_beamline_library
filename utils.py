from epics import PV
from ophyd import Component, Device

from importlib import import_module
from pkgutil import iter_modules, walk_packages
from functools import partial

from types import ModuleType
from ophyd import Device
import saxs_beamline_library

class InvalidDevice(Exception):
    pass


def get_device(address: str) -> Device:
    fragments = address.split('.')
    try:
        for i, f in enumerate(fragments, 1):
            _ = import_module(f'.{".".join(fragments[:i])}', 'saxs_beamline_library.devices')
            result = _
    except ModuleNotFoundError:
        pass
    try:
        for f in fragments[i - 1 :]:
            result = getattr(result, f)
    except (UnboundLocalError, AttributeError):
        raise InvalidDevice(f'"{address}" does not describe a valid device.') from None

    return result



def list_devices() -> list:


    def _list_devices(_modname):
        _devices = []
        submodule = import_module(f'.{_modname}', 'saxs_beamline_library')
        for attr in dir(submodule):
            if attr[0] != '_':
                _ = getattr(submodule, attr)
                if isinstance(_, Device):
                    try:
                        for c in _.component_names:
                            
                            _devices.append(
                                {
                                    'device': f'{_modname}.{attr}.{c}',
                                    'name': get_device(f'{_modname}.{attr}.{c}').name,
                                }
                            )
                    except AttributeError:
                        _devices.append(
                            {'device': f'{_modname}.{attr}', 'name': get_device(f'{_modname}.{attr}').name,}
                        )
        return _devices

    devices = []

    for importer, modname, ispkg in iter_modules(saxs_beamline_library.__path__):
        try:
            if ispkg:
                print(f'saxs_beamline_library.{modname}')
                for _importer, submodname, _ispkg in iter_modules(
                    import_module(f'.{modname}', 'saxs_beamline_library').__path__
                ):
                    devices.extend(_list_devices(f'{modname}.{submodname}'))
            else:
                devices.extend(_list_devices(modname))
        except Exception as e:
            pass
    return devices

