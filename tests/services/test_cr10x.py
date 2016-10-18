import os
import shutil

from services import cr10xformatter
from services import utils

TEST_BASE_DIR = os.path.join(os.path.dirname(__file__))
TEST_CFG_DIR = os.path.join(TEST_BASE_DIR, 'cfg')
TEST_DATA_DIR = os.path.join(TEST_BASE_DIR, 'testdata')
TEST_OUTPUT_DIR = os.path.join(TEST_BASE_DIR, 'testoutput')


def test_process_location():
    config_file = os.path.join(TEST_CFG_DIR, 'cr10x_sample_config.yaml')
    cfg = utils.load_config(config_file)
    site = 'lake'
    location = 'lake_location'
    testdata_file_path = cfg['sites'][site]['locations'][location]['file_path']

    cfg['sites'][site]['locations'][location]['file_path'] = (
        os.path.join(TEST_DATA_DIR, testdata_file_path)
    )

    location_info = cfg['sites'][site]['locations'][location]

    cr10xformatter.process_location(
        cfg=cfg,
        output_dir=TEST_OUTPUT_DIR,
        site=site,
        location=location,
        location_info=location_info
    )

    array_ids = location_info.get('array_ids')

    for array_id, array_id_info in array_ids.items():
        array_name = array_id_info.get('name')
        array_id_path = os.path.join(TEST_OUTPUT_DIR, site, location, array_name + '.dat')

        assert os.path.exists(array_id_path)

    site_dir_path = os.path.join(TEST_OUTPUT_DIR, site)
    shutil.rmtree(site_dir_path)
