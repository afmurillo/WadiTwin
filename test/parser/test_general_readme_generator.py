from datetime import datetime
from pathlib import Path

import pytest
from wntr.network import WaterNetworkModel, Options

from dhalsim.parser.file_generator import GeneralReadmeGenerator, get_mininet_links


@pytest.fixture
def rm_gen(mocker):
    mocker.patch.object(GeneralReadmeGenerator, "__init__", return_value=None)

    return GeneralReadmeGenerator(None)


@pytest.fixture
def water_network_model(mocker):
    wn = mocker.patch.object(WaterNetworkModel, "__init__", return_value=None)
    opts = mocker.patch.object(Options, "__init__", return_value=None)
    opts.time.hydraulic_timestep = 100
    wn.options = opts
    return wn


@pytest.fixture(scope="session")
def tmp_path(tmpdir_factory):
    return tmpdir_factory.mktemp("data").join("general_readme.md")


def test_init(water_network_model, mocker):
    mocker.patch.object(GeneralReadmeGenerator, 'get_readme_path', return_value='')
    general_rm_gen = GeneralReadmeGenerator(Path(__file__).parent.parent / 'auxilary_testing_files'
                                            / 'intermediate.yaml',
                                            datetime(year=2021, month=6, day=1, second=1),
                                            datetime(year=2021, month=6, day=1, second=2),
                                            False, 5, water_network_model, 300)
    assert general_rm_gen.start_time == datetime(year=2021, month=6, day=1, second=1)
    assert general_rm_gen.end_time == datetime(year=2021, month=6, day=1, second=2)
    assert general_rm_gen.batch == False
    assert general_rm_gen.master_time == 5
    assert general_rm_gen.wn == water_network_model


def test_verifies_written(rm_gen, tmp_path, water_network_model):
    rm_gen.readme_path = tmp_path
    rm_gen.intermediate_yaml = {'inp_file': 'map.inp', 'iterations': 1, 'output_path': 'output',
                                'network_topology_type': 'Simple', 'mininet_cli': False,
                                'log_level': 'info', 'demand': 'pdd', 'simulator': 'wntr', 'DQN_Control': False}
    rm_gen.batch = False
    rm_gen.version = '1.0.0'
    rm_gen.master_time = 1
    rm_gen.wn = water_network_model
    rm_gen.start_time = datetime(year=2021, month=6, day=1, second=1)
    rm_gen.end_time = datetime(year=2021, month=6, day=1, second=2)
    rm_gen.write_readme()
    rm_file = open(tmp_path, 'r')
    assert rm_file is not None


def test_get_value(rm_gen):
    rm_gen.intermediate_yaml = {'parameter': 'value'}
    assert rm_gen.get_value('parameter') == "\n\nparameter: value"


def test_get_optional_present(rm_gen):
    rm_gen.intermediate_yaml = {'parameter': 'value'}
    assert rm_gen.get_optional('parameter') == "\n\nparameter: value"


def test_get_optional_absent(rm_gen):
    rm_gen.intermediate_yaml = {'parameter': 'value'}
    assert rm_gen.get_optional('unexisting') == "\n\nunexisting: None"


def test_checkbox_positive(rm_gen):
    rm_gen.intermediate_yaml = {'parameter': {'T2': 2.08023668123243}}
    assert rm_gen.checkbox('parameter') == "\n\n- [x] parameter"


def test_checkbox_negative(rm_gen):
    rm_gen.intermediate_yaml = {'parameter': {}}
    assert rm_gen.checkbox('parameter') == "\n\n- [ ] parameter"


def test_checkbox_absent(rm_gen):
    rm_gen.intermediate_yaml = {}
    assert rm_gen.checkbox('parameter') == "\n\n- [ ] parameter"


def test_get_input_files_w_batch(rm_gen):
    rm_gen.intermediate_yaml = {'batch_simulations': 3,
                                'output_path': Path('tmp/readme/batch0')}
    assert rm_gen.get_input_files() == "\n\n## Input files\n\nInput files have been copied to" \
                                       " ```tmp/readme```. In case any extra files were used," \
                                       " these files will be copied to the /output/configuration" \
                                       " folder as well."


def test_get_input_files_wo_batch(rm_gen):
    rm_gen.intermediate_yaml = {'output_path': Path('tmp/readme')}
    assert rm_gen.get_input_files() == "\n\n## Input files\n\nInput files have been copied to" \
                                       " ```tmp/readme```. In case any extra files were used," \
                                       " these files will be copied to the /output/configuration" \
                                       " folder as well."


def test_get_configuration_parameters(rm_gen):
    rm_gen.intermediate_yaml = {'iterations': 1, 'network_topology_type': 'Simple',
                                'mininet_cli': False, 'log_level': 'info', 'demand': 'pdd', 'DQN_Control':False}
    assert rm_gen.get_configuration_parameters() == "\n\n## Configuration parameters\n\niterations:" \
                                                    " 1\n\nnetwork_topology_type: Simple\n\n" \
                                                    "mininet_cli: False\n\nlog_level: info\n\n"\
                                                    "simulator: wntr\n\n" \
                                                    "demand: pdd\n\nbatch_simulations: None\n\n"\
                                                    "DQN_Control: False"


def test_get_optional_data_parameters_all(rm_gen):
    rm_gen.intermediate_yaml = {'initial_tank_data': [1], 'demand_patterns': [1],
                                'network_loss_data': [1], 'network_delay_data': [1],
                                'network_attacks': [1]}
    assert rm_gen.get_optional_data_parameters() == "\n\n## Initial conditions\n\n- [x] initial_" \
                                                    "tank_data\n\n- [x] demand_patterns\n\n- [x]" \
                                                    " network_loss_data\n\n- [x] network_delay" \
                                                    "_data\n\n- [x] network_attacks"


def test_get_optional_data_parameters_none(rm_gen):
    rm_gen.intermediate_yaml = {}
    assert rm_gen.get_optional_data_parameters() == "\n\n## Initial conditions\n\n- [ ] initial_" \
                                                    "tank_data\n\n- [ ] demand_patterns\n\n- [ ]" \
                                                    " network_loss_data\n\n- [ ] network_delay" \
                                                    "_data\n\n- [ ] network_attacks"


def test_get_optional_data_parameters_some(rm_gen):
    rm_gen.intermediate_yaml = {'initial_tank_data': [1], 'demand_patterns': [1],
                                'network_delay_data': [], 'network_attacks': [1]}
    assert rm_gen.get_optional_data_parameters() == "\n\n## Initial conditions\n\n- [x] initial_" \
                                                    "tank_data\n\n- [x] demand_patterns\n\n- [ ]" \
                                                    " network_loss_data\n\n- [ ] network_delay" \
                                                    "_data\n\n- [x] network_attacks"


def test_get_standalone_parameter_information_batch(rm_gen):
    rm_gen.batch = True
    assert rm_gen.get_standalone_parameter_information() == ""


def test_get_standalone_parameter_information_no_batch_some(rm_gen):
    rm_gen.batch = False
    rm_gen.intermediate_yaml = {'network_loss_values': {'PLC1': 0.1427168730880501},
                                'network_delay_values': {}}
    assert rm_gen.get_standalone_parameter_information() == "\n\n## Network loss values\n\n" \
                                                            "{'PLC1': 0.1427168730880501}"


def test_get_standalone_parameter_information_no_batch_all(rm_gen):
    rm_gen.batch = False
    rm_gen.intermediate_yaml = {'network_loss_values': {'PLC1': 0.1427168730880501},
                                'network_delay_values': {'PLC1': '89.55084013904876ms'},
                                'initial_tank_values': {'T2': 2.08023668123243, 'T3': 2.34}}
    assert rm_gen.get_standalone_parameter_information() == "\n\n## Initial tank values\n\n" \
                                                            "{'T2': 2.08023668123243, 'T3': 2.34}" \
                                                            "\n\n## Network loss values\n\n" \
                                                            "{'PLC1': 0.1427168730880501}" \
                                                            "\n\n## Network delay values\n\n" \
                                                            "{'PLC1': '89.55084013904876ms'}"


def test_get_versioning(rm_gen):
    rm_gen.version = "1.0.0"
    assert rm_gen.get_versioning() == "\n\n## About this experiment\n\nRan with DHALSIM v1.0.0."


def test_get_standalone_iteration_information_batch(rm_gen):
    rm_gen.batch = True
    assert rm_gen.get_standalone_iteration_information() == ""


def test_get_standalone_iteration_information_no_batch(rm_gen, water_network_model):
    rm_gen.batch = False
    rm_gen.master_time = 1
    rm_gen.intermediate_yaml = {'iterations': 5}
    rm_gen.wn = water_network_model
    assert rm_gen.get_standalone_iteration_information() == "\n\nRan for 1 out of 5 iterations " \
                                                            "with hydraulic timestep 100."


def test_get_time_information(rm_gen):
    rm_gen.start_time = datetime(year=2021, month=6, day=1, second=1)
    rm_gen.end_time = datetime(year=2021, month=6, day=1, second=2)
    assert rm_gen.get_time_information() == "\n\nStarted at 2021-06-01 00:00:01 and finished at" \
                                            " 2021-06-01 00:00:02.\n\nThe duration of this" \
                                            " simulation was 0:00:01."


def test_get_mininet_links():
    assert get_mininet_links() == "\n\n## Mininet links\n\nMininet links can be found in the" \
                                  " file mininet_links.md in this configuration folder."
