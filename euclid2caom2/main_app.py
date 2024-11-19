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

"""
This module implements the ObsBlueprint mapping, as well as the workflow entry point that executes the workflow.
"""

from datetime import timedelta
from os.path import basename

from caom2utils.blueprints import _to_float
from caom2 import CalibrationLevel, DataProductType, ProductType, ReleaseType
from caom2pipe.astro_composable import FilterMetadataCache
from caom2pipe import caom_composable as cc
from caom2pipe import manage_composable as mc


__all__ = [
    'EUCLIDMappingNIR',
    'EUCLIDMappingVIS',
    'EUCLIDName',
]


class EUCLIDName(mc.StorageName):
    """Naming rules:
    - support mixed-case file name storage, and mixed-case obs id values
    - support uncompressed files in storage

    Sample file name:
    EUC_MER_BGSUB-MOSAIC-NIR-Y_TILE102163762-7F3874_20240516T064835.674261Z_00.00.fits
    """

    EUCLID_NAME_PATTERN = '*'

    def __init__(self, source_names):
        super().__init__(file_name=basename(source_names[0]), source_names=source_names)
        self._target_name = None
        self._set_target_name()

    def _set_target_name(self):
        bits = self._file_id.split('_')
        if len(bits) > 3:
            self._target_name = bits[3]
    @property
    def target_name(self):
        return self._target_name

    def is_valid(self):
        return True


class EUCLIDMappingNIR(cc.TelescopeMapping2):
    def __init__(self, clients, config, dest_uri, observation, reporter, storage_name):
        self._reporter = reporter
        super().__init__(
            storage_name,
            storage_name.metadata.get(dest_uri),
            clients,
            self._reporter.observable,
            observation,
            config,
    )

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        bp.set('DerivedObservation.members', {})
        bp.set('Observation.algorithm.name', 'stack')
        bp.add_attribute('Observation.metaRelease', 'DATE')
        bp.set('Observation.type', 'object')

        bp.set('Observation.instrument.name', '_get_instrument_name()')
        bp.set('Observation.proposal.id', 'Q1')
        bp.set('Observation.target.name', self._storage_name.target_name)
        bp.add_attribute('Observation.target_position.point.cval1', 'CRVAL1')
        bp.add_attribute('Observation.target_position.point.cval2', 'CRVAL2')
        bp.set('Observation.target_position.coordsys', 'FK5')
        bp.set('Observation.telescope.name', 'Euclid')

        bp.set('Plane.calibrationLevel', CalibrationLevel.ANALYSIS_PRODUCT)
        bp.set('Plane.dataProductType', DataProductType.IMAGE)
        bp.set('Plane.dataRelease', '2030-01-01T00:00:00.000')
        bp.add_attribute('Plane.metaRelease', 'DATE')
        bp.add_attribute('Plane.provenance.name', 'SOFTNAME')
        bp.add_attribute('Plane.provenance.version', 'SOFTVERS')
        bp.add_attribute('Plane.provenance.project', 'SOFTAUTH')
        bp.add_attribute('Plane.provenance.producer', 'ORIGIN')
        bp.set('Plane.provenance.reference', '_get_provenance_reference()')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')

        bp.set('Artifact.productType', ProductType.SCIENCE)
        bp.set('Artifact.releaseType', ReleaseType.META)

        bp.configure_position_axes((1, 2))
        # from https://www.euclid-ec.org/science/overview/#
        # VIS
        # pixel scale: 0.1 arcsecond
        # FoV: 0.57 degrees squared
        #
        # NISP
        # pixel scale: 0.3 arcsecond
        bp.set('Chunk.position.resolution', 0.3)
        #
        bp.configure_energy_axis(3)
        bp.set('Chunk.energy.axis.axis.ctype', 'WAVE')
        bp.set('Chunk.energy.axis.axis.cunit', 'Angstrom')
        bp.set('Chunk.energy.axis.function.delta', '_get_energy_function_delta()')
        bp.set('Chunk.energy.axis.function.naxis', 1.0)
        bp.set('Chunk.energy.axis.function.refCoord.pix', 1.0)
        bp.set('Chunk.energy.axis.function.refCoord.val', '_get_energy_function_val()')
        bp.clear('Chunk.energy.bandpassName')
        bp.add_attribute('Chunk.energy.bandpassName', 'FILTER')
        bp.set('Chunk.energy.resolvingPower', '_get_energy_resolving_power()')
        bp.set('Chunk.energy.specsys', 'TOPOCENT')
        bp.set('Chunk.energy.ssysobs', 'TOPOCENT')
        bp.set('Chunk.energy.ssyssrc', 'TOPOCENT')
        self._logger.debug('Done accumulate_bp.')

    def _get_energy_function_delta(self, ext):
        result = None
        filter_name = self._headers[ext].get('FILTER')
        if filter_name:
            temp = get_filter_md(filter_name)
            result = FilterMetadataCache.get_fwhm(temp)
        return result

    def _get_energy_function_val(self, ext):
        result = None
        filter_name = self._headers[ext].get('FILTER')
        if filter_name:
            temp = get_filter_md(filter_name)
            result = FilterMetadataCache.get_central_wavelength(temp)
        return result

    def _get_energy_resolving_power(self, ext):
        result = None
        delta = _to_float(self._get_energy_function_delta(ext))
        val = _to_float(self._get_energy_function_val(ext))
        if delta is not None and val is not None:
            result = val / delta
        return result

    def _get_instrument_name(self, ext):
        result = None
        filter = self._headers[ext].get('FILTER')
        if filter:
            result = filter[0:3]
        return result

    def _get_provenance_reference(self, ext):
        result = None
        softinst = self._headers[ext].get('SOFTINST')
        if softinst:
            result = softinst.split()[-1]
        return result

    def _get_two_years_from_DATE(self, ext):
        d_future = None
        d = self._headers[ext].get('DATE')
        if d:
            d_dt = mc.make_datetime(d)
            d_future = d_dt + timedelta(days=2*365)
        return d_future

    def _update_artifact(self, artifact):
        for part in artifact.parts.values():
            for chunk in part.chunks:
                # not for cut-outs
                chunk.energy_axis = None


class EUCLIDMappingVIS(EUCLIDMappingNIR):

    def __init__(self, clients, config, dest_uri, observation, reporter, storage_name):
        super().__init__(clients, config, dest_uri, observation, reporter, storage_name)

    def accumulate_blueprint(self, bp):
        super().accumulate_blueprint(bp)
        # from https://www.euclid-ec.org/science/overview/#
        # VIS
        # pixel scale: 0.1 arcsecond
        # FoV: 0.57 degrees squared
        bp.set('Chunk.position.resolution', 0.1)

def get_filter_md(filter_name):
    filter_md = filter_cache.get_svo_filter(filter_name[0:3], filter_name)
    if not filter_cache.is_cached(filter_name[0:3], filter_name):
        # want to stop ingestion if the filter name is not expected
        raise mc.CadcException(f'Could not find filter metadata for {filter_name}.')
    return filter_md


FILTER_REPAIR_LOOKUP = {
    'NIR_Y': 'NISP.Y',
    'NIR_J': 'NISP.J',
    'NIR_H': 'NISP.H',
    'VIS': 'VIS.vis',
}

INSTRUMENT_REPAIR_LOOKUP = {}

filter_cache = FilterMetadataCache(
    repair_filter_lookup=FILTER_REPAIR_LOOKUP,
    repair_instrument_lookup=INSTRUMENT_REPAIR_LOOKUP,
    telescope='Euclid',
    cache={},
)
