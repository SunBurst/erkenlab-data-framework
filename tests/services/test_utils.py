import os

import pytest

from services import utils

TEST_CFG_DIR = os.path.join(os.path.dirname(__file__), 'cfg')
TEST_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'testoutput')


def delete_output_file(file_path):
    os.remove(file_path)


def test_load_config_empty():
    config_file = os.path.join(TEST_CFG_DIR, 'empty_config.yaml')
    cfg = utils.load_config(config_file)

    assert not cfg


def test_load_config_content():
    config_file = os.path.join(TEST_CFG_DIR, 'sample_config.yaml')
    cfg = utils.load_config(config_file)

    expected_cfg_dict = {
        'root': {
            'title': 'test_title',
            'child_1': {
                'child_1_child_1': {
                    'test_true': True,
                    'test_false': False,
                    'list_1': ['list_item_1', 'list_item_2', 'list_item_3']
                }
            },
            'child_2': {
                'child_2_child_1': {
                    'test_dict': {
                        'dict_key_1': 'dict_value_1',
                        'dict_key_2': 'dict_value_2'
                    }
                }
            }
        }
    }

    assert cfg == expected_cfg_dict


def test_save_config_content():
    output_config_file = os.path.join(TEST_OUTPUT_DIR, 'output_config.yaml')
    cfg = {'root': 'test_title'}

    utils.save_config(output_config_file, cfg)
    assert os.path.exists(output_config_file)

    written_cfg = utils.load_config(output_config_file)
    assert cfg == written_cfg

    delete_output_file(output_config_file)


def test_clean_data_output_dir():
    test_files = ['a.dat', 'b.csv', 'c.ini']
    test_file_extensions = ['*.dat', '*.csv', '*.ini']

    for file in test_files:
        test_file_path = os.path.join(TEST_OUTPUT_DIR, file)
        with open(test_file_path, 'w+') as f:
            assert os.path.exists(test_file_path)

    utils.clean_data_output_dir(TEST_OUTPUT_DIR, *test_file_extensions)

    for file in test_files:
        test_file_path = os.path.join(TEST_OUTPUT_DIR, file)
        assert not os.path.exists(test_file_path)


def test_round_of_rating_valid_ratings():
    test_1_expected_result = 2.5
    test_1_result = utils.round_of_rating(number=2.7, rating=0.5)
    assert test_1_result == test_1_expected_result

    test_2_expected_result = 1.75
    test_2_result = utils.round_of_rating(number=1.65, rating=0.25)
    assert test_2_result == test_2_expected_result

    test_3_expected_result = 4
    test_3_result = utils.round_of_rating(number=3.51, rating=1.0)
    assert test_3_result == test_3_expected_result

    test_4_expected_result = 3.75
    test_4_result = utils.round_of_rating(number=3.8, rating=0.175)
    assert test_4_result == test_4_expected_result


def test_round_of_rating_invalid_rating():
    with pytest.raises(utils.InvalidRatingValueError):
        utils.round_of_rating(number=1.55, rating=0.33)
