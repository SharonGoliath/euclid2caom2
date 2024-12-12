# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2024.                            (c) 2024.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

from mock import Mock, patch

from astropy.io.votable import parse_single_table
from caom2utils.data_util import get_local_file_headers, get_local_file_info
from caom2pipe.execute_composable import MetaVisitRunnerMeta
from euclid2caom2 import file2caom2_augmentation, main_app
from caom2.diff import get_differences
from caom2pipe import manage_composable as mc

import glob
import logging
import os
import traceback


def pytest_generate_tests(metafunc):
    obs_id_list = glob.glob(f'{metafunc.config.invocation_dir}/data/tile*')
    metafunc.parametrize('test_name', obs_id_list)


@patch('caom2pipe.astro_composable.get_vo_table')
def test_main_app(svo_mock, test_name, test_config, tmp_path, change_test_dir):
    test_set = glob.glob(f'{test_name}/*.fits.header')
    svo_mock.side_effect = _svo_mock
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.use_local_files = False
    test_config.log_to_file = True
    test_config.data_sources = [tmp_path.as_posix()]
    test_config.change_working_directory(tmp_path.as_posix())
    test_config.proxy_file_name = 'test_proxy.pem'
    test_config.logging_level = 'ERROR'
    test_config.write_to_file(test_config)

    test_reporter = mc.ExecutionReporter(test_config, mc.Observable(test_config))

    expected_fqn = f'{test_name}/{os.path.basename(test_name)}.expected.xml'
    in_fqn = expected_fqn.replace('.expected', '.in')
    actual_fqn = expected_fqn.replace('expected', 'actual')
    if os.path.exists(actual_fqn):
        os.unlink(actual_fqn)

    observation = None
    if os.path.exists(in_fqn):
        observation = mc.read_obs_from_file(in_fqn)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    clients_mock = Mock()
    test_subject = MetaVisitRunnerMeta(clients_mock, test_config, [file2caom2_augmentation], test_reporter)
    test_subject._observation = observation

    for entry in test_set:
        def _mock_repo_read(collection, obs_id):
            return test_subject._observation
        clients_mock.metadata_client.read.side_effect = _mock_repo_read

        def _read_header_mock(ignore1):
            return get_local_file_headers(entry)
        clients_mock.data_client.get_head.side_effect = _read_header_mock

        def _info_mock(uri):
            temp = get_local_file_info(entry)
            temp.file_type = 'application/fits'
            return temp
        clients_mock.data_client.info.side_effect = _info_mock

        storage_name = main_app.EUCLIDName(source_names=[entry])
        context = {'storage_name': storage_name}
        try:
            test_subject.execute(context)
        except mc.CadcException as e:
            logging.error(traceback.format_exc())
            assert False

    if test_subject._observation:
        if os.path.exists(expected_fqn):
            expected = mc.read_obs_from_file(expected_fqn)
            try:
                compare_result = get_differences(expected, test_subject._observation)
            except Exception as e:
                mc.write_obs_to_file(test_subject._observation, actual_fqn)
                assert False, f'{e}'
            if compare_result is not None:
                mc.write_obs_to_file(test_subject._observation, actual_fqn)
                compare_text = '\n'.join([r for r in compare_result])
                raise AssertionError(
                    f'Differences found in observation {expected.observation_id}\n{compare_text}.\nCheck {actual_fqn}'
                )
        else:
            raise AssertionError(f'No expected observation here {expected_fqn}. See actual here {actual_fqn}')
    else:
        raise AssertionError(f'No observation created for comparison with {expected_fqn}')

    # assert False  # cause I want to see logging messages


def _svo_mock(url):
    try:
        x = url.split('/')
        filter_name = x[-1].replace('&VERB=0', '')
        votable = parse_single_table(f'{os.path.dirname(os.path.realpath(__file__))}/data/filters/{filter_name}.xml')
        return votable, None
    except Exception as _:
        logging.error(f'get_vo_table failure for url {url}')
