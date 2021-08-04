import sys
from pathlib import Path

import pytest
from schema import SchemaError

from dhalsim.parser.config_parser import ConfigParser, SchemaParser


def test_python_version():
    assert sys.version_info.major is 3


@pytest.fixture
def test_dict():
    return {
        "inp_file": Path(),
        "network_topology_type": "simple",
        "simulator":"wntr",
        "output_path": Path(),
        "iterations": 10,
        "mininet_cli": False,
        "DQN_Control": False,
        "log_level": "info",
        "demand": "pdd",
        "noise_scale": 0.1,
        "batch_simulations": 3,
        "saving_interval": 3,
        "initial_tank_data": Path(),
        "demand_patterns": Path(),
        "network_loss_data": Path(),
        "network_delay_data": Path(),
        "plcs": [
            {"name": "PLC1", "sensors": ["T0"], "actuators": ["P_RAW1", "V_PUB"]},
            {"name": "PLC2", "sensors": ["T2"], "actuators": ["V_ER2i"]},
        ],

        "attacks": {
            "device_attacks": [
                {
                    "name": "Close_PRAW1_from_iteration_5_to_10",
                    "trigger": {
                        "type": "Time",
                        "start": 5,
                        "end": 10
                    },
                    "actuator": "P_RAW1",
                    "command": "closed",
                }
            ]
        },
    }


@pytest.fixture
def attack_dict_1():
    return {
        'name': 'test1',
        'tags': [{'tag': 'T2', 'value': 0.2}],
        'target': 'PLC2',
        'trigger': {'end': 7, 'start': 2, 'type': 'Time'},
        'type': 'mitm'
    }


@pytest.fixture
def attack_dict_2():
    return {'name': 'test2',
            'target': 'PLC1',
            'trigger': {'end': 17, 'start': 12, 'type': 'Time'},
            'type': 'naive_mitm',
            'value': -2}


@pytest.fixture
def attack_dict_3():
    return {
        'name': 'test1',
        'actuator': 'P_RAW1',
        'trigger': {'type': "Between",
                    'sensor': "T2",
                    'lower_value': 0.10,
                    'upper_value': 0.16},
        'command': 'closed'
    }


@pytest.fixture
def attack_dict_4():
    return {'name': 'test2',
            'actuator': 'P_RAW1',
            'trigger': {'end': 17, 'start': 12, 'type': 'Time'},
            'command': 'open'
            }


def test_valid_dict(test_dict):
    SchemaParser.validate_schema(test_dict)
    assert True


@pytest.mark.parametrize("key, default_value", [
    ('network_topology_type', 'simple'),
    ('mininet_cli', False),
    ('log_level', 'info'),
    ('simulator', 'wntr'),
    ('DQN_Control', False),
    ('demand', 'pdd')
])
def test_default_config(key, default_value, test_dict):
    del test_dict[key]
    result = SchemaParser.validate_schema(test_dict)
    assert result[key] == default_value


@pytest.mark.parametrize("key", [
    'iterations',
    'batch_simulations',
    'initial_tank_data',
    'demand_patterns',
    'saving_interval',
    'network_loss_data',
    'network_delay_data',
    'attacks',
])
def test_optional_config(key, test_dict):
    del test_dict[key]
    result = SchemaParser.validate_schema(test_dict)
    assert result.get(key) is None


@pytest.mark.parametrize("required_key", ['inp_file', 'plcs'])
def test_required_config(required_key, test_dict):
    del test_dict[required_key]
    with pytest.raises(SchemaError):
        SchemaParser.validate_schema(test_dict)


@pytest.mark.parametrize("key, invalid_value", [
    ('network_topology_type', 2),
    ('network_topology_type', 'invalid'),
    ('network_topology_type', ''),
    ('iterations', '5'),
    ('iterations', 10.5),
    ('iterations', -10),
    ('iterations', 0),
    ('mininet_cli', "False"),
    ('mininet_cli', "True"),
    ('mininet_cli', ""),
    ('mininet_cli', 1),
    ('mininet_cli', 0),
    ('log_level', 1),
    ('log_level', "invalid"),
    ('log_level', ""),
    ('demand', 1),
    ('demand', "invalid"),
    ('demand', ""),
    ('simulator', 1),
    ('simulator', "invalid"),
    ('simulator', ""),
    ('batch_simulations', '5'),
    ('batch_simulations', 10.5),
    ('batch_simulations', -10),
    ('batch_simulations', 0),
    ('saving_interval', 0),
    ('saving_interval', -2),
    ('saving_interval', 2.5),
    ('saving_interval', '3'),
    ('noise_scale', -1.0),
    ('noise_scale', '1'),
    ('DQN_Control', "False"),
    ('DQN_Control', "True"),
    ('DQN_Control', ""),
    ('DQN_Control', 1),
    ('DQN_Control', 0),
])
def test_invalid_config(key, invalid_value, test_dict):
    test_dict[key] = invalid_value
    with pytest.raises(SchemaError):
        SchemaParser.validate_schema(test_dict)


@pytest.mark.parametrize("key, input_value, expected_value", [
    ('network_topology_type', 'simple', 'simple'),
    ('network_topology_type', 'SIMPLE', 'simple'),
    ('network_topology_type', 'complex', 'complex'),
    ('network_topology_type', 'COMPLEX', 'complex'),
    ('iterations', 100, 100),
    ('mininet_cli', True, True),
    ('mininet_cli', False, False),
    ('log_level', 'debug', 'debug'),
    ('log_level', 'DEBUG', 'debug'),
    ('log_level', 'info', 'info'),
    ('log_level', 'INFO', 'info'),
    ('log_level', 'warning', 'warning'),
    ('log_level', 'WARNING', 'warning'),
    ('log_level', 'error', 'error'),
    ('log_level', 'ERROR', 'error'),
    ('log_level', 'critical', 'critical'),
    ('log_level', 'CRITICAL', 'critical'),
    ('demand', 'pdd', 'pdd'),
    ('demand', 'PDD', 'pdd'),
    ('demand', 'dd', 'dd'),
    ('demand', 'DD', 'dd'),
    ('simulator', 'wntr', 'wntr'),
    ('simulator', 'WNTR', 'wntr'),
    ('simulator', 'epynet', 'epynet'),
    ('simulator', 'EPYNET', 'epynet'),
    ('batch_simulations', 100, 100),
    ('saving_interval', 2, 2),
    ('noise_scale', 0.0, 0.0),
    ('DQN_Control', True, True),
    ('DQN_Control', False, False),
])
def test_valid_config(key, input_value, expected_value, test_dict):
    test_dict[key] = input_value
    output = SchemaParser.validate_schema(test_dict)
    assert output[key] == expected_value


@pytest.mark.parametrize("key", [
    'actuators',
    'sensors',
])
def test_optional_plcs(key, test_dict):
    del test_dict['plcs'][0][key]
    result = SchemaParser.validate_schema(test_dict)
    assert result.get(key) is None


@pytest.mark.parametrize("required_key", ['name'])
def test_required_plcs(required_key, test_dict):
    del test_dict['plcs'][0][required_key]
    with pytest.raises(SchemaError):
        SchemaParser.validate_schema(test_dict)


@pytest.mark.parametrize("key, invalid_value", [
    ('name', ''),
    ('name', 'PLC 1'),
    ('name', 1),
    ('name', 0),
    ('name', 'plc#1'),
    ('name', '&'),
    ('name', ' plc1'),
    ('sensors', ["T 0"]),
    ('sensors', [" T0"]),
    ('sensors', ["T#0"]),
    ('sensors', [5]),
    ('sensors', [4.2]),
    ('sensors', ['&']),
    ('sensors', ["木头"]),
    ('actuators', ["V 0"]),
    ('actuators', [" V0"]),
    ('actuators', ["V#0"]),
    ('actuators', [5]),
    ('actuators', [4.2]),
    ('actuators', ['&']),
    ('actuators', ["木头"]),
])
def test_invalid_plc(key, invalid_value, test_dict):
    test_dict['plcs'][0][key] = invalid_value
    with pytest.raises(SchemaError):
        SchemaParser.validate_schema(test_dict)


@pytest.mark.parametrize("key, input_value, expected_value", [
    ('name', 'plc1', 'plc1'),
    ('name', 'PLc1', 'PLc1'),
    ('name', 'PLC100', 'PLC100'),
    ('name', 'thing', 'thing'),
    ('name', '123', '123'),
    ('name', 'plc_1', 'plc_1'),
    ('sensors', ["T0", "T1"], ["T0", "T1"]),
    ('sensors', ["Tank"], ["Tank"]),
    ('sensors', [], []),
    ('sensors', ["tank_42"], ["tank_42"]),
    ('actuators', ["V0", "V1"], ["V0", "V1"]),
    ('actuators', ["Valve"], ["Valve"]),
    ('actuators', [], []),
    ('actuators', ["valve_42"], ["valve_42"]),
])
def test_valid_plc(key, input_value, expected_value, test_dict):
    test_dict['plcs'][0][key] = input_value
    output = SchemaParser.validate_schema(test_dict)
    assert output['plcs'][0][key] == expected_value


def test_attack_dicts(attack_dict_1, attack_dict_2, attack_dict_3, attack_dict_4):
    SchemaParser.network_attacks.validate(attack_dict_1)
    SchemaParser.network_attacks.validate(attack_dict_2)

    SchemaParser.device_attacks.validate(attack_dict_3)
    SchemaParser.device_attacks.validate(attack_dict_4)


@pytest.mark.parametrize("key, input_value", [
    ('type', 9),
    ('type', 'hi'),
    ('trigger', 4),
    ('name', 'device attack'),
    ('name', 'network attack 5+10'),
    ('trigger', {'type': 'Time', 'start': '5', 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': '10'}),
    ('tags', 3),
    ('tags', [{'tag': 'T2', 'value': 'not_int'}]),
    ('tags', [{'tag': 8, 'value': 0.2}]),
    ('target', 5),
    ('wrong', 1)
])
def test_invalid_device_attacks(key, input_value, attack_dict_3, attack_dict_4):
    attack_dict_3[key] = input_value
    with pytest.raises(SchemaError):
        SchemaParser.device_attacks.validate(attack_dict_3)
    attack_dict_4[key] = input_value
    with pytest.raises(SchemaError):
        SchemaParser.device_attacks.validate(attack_dict_4)


@pytest.mark.parametrize("key, input_value, expected", [
    ('name', 'hello', 'hello'),
    ('name', 'device_attack', 'device_attack'),
    ('name', 'network_attack_5->10', 'network_attack_5->10'),
    ('name', 'close_when_5<T0<10', 'close_when_5<T0<10'),
    ('name', '10', '10'),
    ('trigger', {'type': 'Time', 'start': 5, 'end': 10}, {'type': 'time', 'start': 5, 'end': 10}),
])
def test_valid_device_attacks(key, input_value, expected, attack_dict_3, attack_dict_4):
    attack_dict_3[key] = input_value
    output = SchemaParser.device_attacks.validate(attack_dict_3)
    assert output[key] == expected
    attack_dict_4[key] = input_value
    output = SchemaParser.device_attacks.validate(attack_dict_4)
    assert output[key] == expected


@pytest.mark.parametrize("key, input_value", [
    ('type', 9),
    ('type', 'hi'),
    ('trigger', 4),
    ('name', 'network attack'),
    ('name', 'network_attack_5<10'),
    ('name', 'networkattack_5to10'),
    ('trigger', {'type': 'Time', 'start': '5', 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': '10'}),
    ('tags', 3),
    ('tags', [{'tag': 'T2', 'value': 'not_int'}]),
    ('tags', [{'tag': 8, 'value': 0.2}]),
    ('target', 5),
    ('wrong', 1)
])
def test_invalid_network_attacks_mitm(key, input_value, attack_dict_1):
    attack_dict_1[key] = input_value
    with pytest.raises(SchemaError):
        SchemaParser.network_attacks.validate(attack_dict_1)


@pytest.mark.parametrize("key, input_value, expected", [
    ('name', 'hello', 'hello'),
    ('name', 'network_at', 'network_at'),
    ('name', 'nwk_atk___', 'nwk_atk___'),
    ('name', '10', '10'),
    ('name', 'atak', 'atak'),
    ('trigger', {'type': 'Time', 'start': 5, 'end': 10}, {'type': 'time', 'start': 5, 'end': 10}),
    ('tags', [{'tag': 'T2', 'value': 0.2}], [{'tag': 'T2', 'value': 0.2}]),
])
def test_valid_network_attacks_mitm(key, input_value, expected, attack_dict_1):
    attack_dict_1[key] = input_value
    output = SchemaParser.network_attacks.validate(attack_dict_1)
    assert output[key] == expected


@pytest.mark.parametrize("key, input_value", [
    ('type', 9),
    ('type', 'hi'),
    ('name', 9),
    ('trigger', 4),
    ('name', 'network attack'),
    ('name', 'network_attack_5<10'),
    ('name', 'networkattack_5to10'),
    ('trigger', {'type': 'Time', 'start': '5', 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': 10}),
    ('trigger', {'type': 'NoType', 'start': 5, 'end': '10'}),
    ('target', True),
    ('wrong', 1)
])
def test_invalid_attacks_naive(key, input_value, attack_dict_2):
    attack_dict_2[key] = input_value
    with pytest.raises(SchemaError):
        SchemaParser.network_attacks.validate(attack_dict_2)


@pytest.mark.parametrize("key, input_value, expected", [
    ('name', 'hello', 'hello'),
    ('name', 'network_at', 'network_at'),
    ('name', 'nwk_atk___', 'nwk_atk___'),
    ('name', '10', '10'),
    ('name', 'atak', 'atak'),
    ('trigger', {'type': 'Time', 'start': 5, 'end': 10}, {'type': 'time', 'start': 5, 'end': 10}),
])
def test_valid_attacks_naive(key, input_value, expected, attack_dict_2):
    attack_dict_2[key] = input_value
    output = SchemaParser.network_attacks.validate(attack_dict_2)
    assert output[key] == expected


@pytest.mark.parametrize("required_key", [
    'name',
    'type',
    'trigger',
    'target'
])
def test_required_attack_fields_mitm(required_key, attack_dict_1):
    del attack_dict_1[required_key]
    with pytest.raises(SchemaError):
        SchemaParser.network_attacks.validate(attack_dict_1)


@pytest.mark.parametrize("required_key", [
    'name',
    'type',
    'trigger',
    'target'
])
def test_required_attack_fields_naive(required_key, attack_dict_2):
    del attack_dict_2[required_key]
    with pytest.raises(SchemaError):
        SchemaParser.network_attacks.validate(attack_dict_2)


@pytest.mark.parametrize("trigger, expected", [
    ({'type': 'Time', 'start': 5, 'end': 10},{'type': 'time', 'start': 5, 'end': 10}),
    ({'type': 'Above', 'sensor': 'T1', 'value': 3.0},{'type': 'above', 'sensor': 'T1', 'value': 3.0}),
    ({'type': 'Below', 'sensor': 't_5', 'value': 3.0},{'type': 'below', 'sensor': 't_5', 'value': 3.0}),
    ({'type': 'Between', 'sensor': 't_5', 'lower_value': 3.0, 'upper_value': 4.0},{'type': 'between', 'sensor': 't_5', 'lower_value': 3.0, 'upper_value': 4.0}),
    ({'type': 'Above', 'sensor': 'T1', 'value': 3},{'type': 'above', 'sensor': 'T1', 'value': 3.0}),
    ({'type': 'Below', 'sensor': 't_5', 'value': 3},{'type': 'below', 'sensor': 't_5', 'value': 3.0}),
    ({'type': 'Between', 'sensor': 't_5', 'lower_value': 3, 'upper_value': 4.0},{'type': 'between', 'sensor': 't_5', 'lower_value': 3.0, 'upper_value': 4.0}),
    ({'type': 'Between', 'sensor': 't_5', 'lower_value': 3.0, 'upper_value': 4},{'type': 'between', 'sensor': 't_5', 'lower_value': 3.0, 'upper_value': 4.0}),
])
def test_valid_trigger(trigger, expected):
    assert SchemaParser.trigger.validate(trigger) == expected


@pytest.mark.parametrize("trigger", [
    {'type': 'Time', 'start': 5},
    {'type': 'Time', 'start': 5, 'end': "10"},
    {'type': 'Time', 'start': "5", 'end': 10},
    {'type': 'Time',  'end': 10},
    {'type': 'Above', 'sensor': 'T1', 'value': '3.0'},
    {'type': 'Below', 'sensor': 't_5', 'value': '3.0'},
    {'type': 'Above', 'value': 3.0},
    {'type': 'Below', 'sensor': 't_5'},
    {'type': 'Below', 'value': 3.0},
    {'type': 'Above', 'sensor': 't_5'},
    {'type': 'Between', 'sensor': 't_5', 'lower_value': '3.0', 'upper_value': 4.0},
    {'type': 'Between', 'lower_value': '3.0', 'upper_value': 4.0},
    {'type': 'Between', 'sensor': 't_5', 'upper_value': 4.0},
    {'type': 'Between', 'sensor': 't_5', 'lower_value': '3.0'},
])
def test_invalid_trigger(trigger):
    with pytest.raises(SchemaError):
        SchemaParser.trigger.validate(trigger)
