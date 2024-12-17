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

from caom2 import CalibrationLevel, Chunk, DataProductType, ProductType, ReleaseType, TypedList
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

    One tile: TILE102070858
    4 filters: VIS Y J H
    loop over 4 filter:
        associated imagey type files for 1 filter:
        esa:EUCLID/EUC_MER_BGSUB-MOSAIC-VIS_TILE102070858-5ED2D5_20241105T125727.727353Z_00.00.fits image
        esa:EUCLID/EUC_MER_BGMOD-VIS_TILE102070858-F79595_20241105T125727.727179Z_00.00.fits background map
        esa:EUCLID/EUC_MER_GRID-PSF-VIS_TILE102070858-7333BC_20241104T161703.183167Z_00.00.fits PSF map
        esa:EUCLID/EUC_MER_MOSAIC-VIS-RMS_TILE102070858-BB87CE_20241104T161703.183124Z_00.00.fits weight map
        esa:EUCLID/EUC_MER_MOSAIC-VIS-FLAG_TILE102070858-B260B9_20241104T161703.183145Z_00.00.fits flag/mask map
        per filter catalog
        esa:EUCLID/EUC_MER_CATALOG-PSF-VIS_TILE102070858-F69E9E_20241105T134129.112687Z_00.00.fits
    per tile catalogs (all 4 bands)
        esa:EUCLID/EUC_MER_FINAL-CAT_TILE102070858-FCBD03_20241106T175237.497132Z_00.00.fits
        esa:EUCLID/EUC_MER_FINAL-CUTOUTS-CAT_TILE102070858-755293_20241106T105450.172109Z_00.00.fits
        esa:EUCLID/EUC_MER_FINAL-MORPH-CAT_TILE102070858-46295E_20241106T175236.702482Z_00.00.fits


    ProductType auxiliary - SGw - 11-12-24
    esa:EUCLID/EUC_MER_BGMOD-VIS_TILE102070858-F79595_20241105T125727.727179Z_00.00.fits
    esa:EUCLID/EUC_MER_GRID-PSF-VIS_TILE102070858-7333BC_20241104T161703.183167Z_00.00.fits
    esa:EUCLID/EUC_MER_MOSAIC-VIS-FLAG_TILE102070858-B260B9_20241104T161703.183145Z_00.00.fits

    """

    EUCLID_NAME_PATTERN = '*'

    def __init__(self, source_names):
        super().__init__(file_name=basename(source_names[0]), source_names=source_names)

    def get_filter_name(self):
        result = self._product_id.split('_')[-1]
        if result not in ['H', 'J', 'Y', 'VIS']:
            raise mc.CadcException(f'Calling get_filter_name when it cannot answer correctly.')
        return result

    def is_auxiliary(self):
        return (
            '-CAT_' in self._file_name
            or '_CATALOG-' in self._file_name
            or '_BGMOD-' in self._file_name
            or '_GRID-PSF-' in self._file_name
            or '-FLAG_' in self._file_name
        )

    def is_valid(self):
        return True

    def is_weight(self):
        return '_MOSAIC-' in self._file_name and '-RMS_' in self._file_name

    def set_obs_id(self, **kwargs):
        bits = self._file_name.split('_')
        self._obs_id = bits[3].split('-')[0]
        if not self._obs_id.startswith('TILE'):
            raise mc.CadcException(f'Unexpected naming pattern {self._file_name}')

    def set_product_id(self, **kwargs):
        bits = self._file_name.split('_')
        product_id_bits = bits[2].split('-')
        if product_id_bits[-1] == 'CAT' and len(product_id_bits) == 3:
            self._product_id = f'{self._obs_id}_{'_'.join(product_id_bits[-2:])}'
        else:
            if product_id_bits[0] == 'MOSAIC':
                self._product_id = f'{self._obs_id}_{product_id_bits[-2]}'
            else:
                self._product_id = f'{self._obs_id}_{product_id_bits[-1]}'


class EUCLIDMappingAuxiliary(cc.TelescopeMapping2):
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
        # SGw - 10-12-24
        bp.set('Observation.algorithm.name', 'OU-MER')
        bp.add_attribute('Observation.metaRelease', 'DATE')
        bp.set('Observation.type', 'object')

        bp.set('Observation.instrument.name', '_get_instrument_name()')
        bp.set('Observation.proposal.id', 'Q1')
        bp.set('Observation.target.name', self._storage_name.obs_id)
        bp.add_attribute('Observation.target_position.point.cval1', 'CRVAL1')
        bp.add_attribute('Observation.target_position.point.cval2', 'CRVAL2')
        bp.set('Observation.target_position.coordsys', 'FK5')
        bp.set('Observation.telescope.name', 'Euclid')

        bp.set('Plane.calibrationLevel', CalibrationLevel.ANALYSIS_PRODUCT)
        bp.set('Plane.dataProductType', DataProductType.CATALOG)
        bp.set('Plane.dataRelease', '2030-01-01T00:00:00.000')
        bp.add_attribute('Plane.metaRelease', 'DATE')
        bp.set('Plane.provenance.name', 'Euclid OU-MER')
        # # SGw 12-12-24
        bp.set('Plane.provenance.project', 'Euclid OU-MER')
        bp.set('Plane.provenance.reference', 'https://www.euclid-ec.org')

        bp.set('Artifact.productType', ProductType.AUXILIARY)
        bp.set('Artifact.releaseType', ReleaseType.DATA)

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
        pass

    def update(self):
        self._observation = super().update()
        cat_plane_key = f'{self._observation.observation_id}_CAT'
        morph_cat_plane_key = f'{self._observation.observation_id}_MORPH_CAT'
        cutouts_cat_plane_key = f'{self._observation.observation_id}_CUTOUTS_CAT'
        cat_keys = [cat_plane_key, morph_cat_plane_key, cutouts_cat_plane_key]
        # TODO the 4th catalogue Plane
        for plane in self._observation.planes.values():
            if plane.product_id not in cat_keys:
                for cat_key in cat_keys:
                    if (
                        cat_key in self._observation.planes.keys()
                        and self._observation.planes[plane.product_id].meta_release is not None
                        and self._observation.planes[cat_key].meta_release is None
                    ):
                        self._observation.planes[cat_key].meta_release = plane.meta_release
        return self._observation


class EUCLIDMappingNIR(EUCLIDMappingAuxiliary):
    def __init__(self, clients, config, dest_uri, observation, reporter, storage_name):
        super().__init__(
            clients,
            config,
            dest_uri,
            observation,
            reporter,
            storage_name,
        )

    def accumulate_blueprint(self, bp):
        """Configure the telescope-specific ObsBlueprint at the CAOM model Observation level."""
        self._logger.debug('Begin accumulate_bp.')
        super().accumulate_blueprint(bp)
        bp.set('Plane.dataProductType', DataProductType.IMAGE)
        bp.clear('Plane.provenance.name')
        bp.add_attribute('Plane.provenance.name', 'SOFTNAME')
        bp.add_attribute('Plane.provenance.version', 'SOFTVERS')
        # SGw 12-12-24
        bp.set('Plane.provenance.project', 'Euclid OU-MER')
        bp.add_attribute('Plane.provenance.producer', 'ORIGIN')
        bp.set('Plane.provenance.reference', '_get_provenance_reference()')
        bp.add_attribute('Plane.provenance.lastExecuted', 'DATE')
        bp.set('Artifact.productType', self._get_artifact_product_type())

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
        bp.set('Chunk.energy.specsys', 'TOPOCENT')
        bp.set('Chunk.energy.ssysobs', 'TOPOCENT')
        bp.set('Chunk.energy.ssyssrc', 'TOPOCENT')
        self._logger.debug('Done accumulate_bp.')

    def _get_artifact_product_type(self):
        result = ProductType.SCIENCE
        if self._storage_name.is_weight():
            result = ProductType.WEIGHT
        return result

    def _get_energy_function_delta(self, ext):
        result = None
        filter_name = self._headers[ext].get('FILTER')
        if not filter_name:
            filter_name = self._storage_name.get_filter_name()
        if filter_name:
            temp = get_filter_md(filter_name)
            result = FilterMetadataCache.get_fwhm(temp)
        return result

    def _get_energy_function_val(self, ext):
        result = None
        filter_name = self._headers[ext].get('FILTER')
        if not filter_name:
            filter_name = self._storage_name.get_filter_name()
        if filter_name:
            temp = get_filter_md(filter_name)
            result = FilterMetadataCache.get_central_wavelength(temp)
        return result

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
        bp.set_default('Chunk.energy.bandpassName', 'VIS')

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

INSTRUMENT_REPAIR_LOOKUP = {'NIR': 'NISP'}

filter_cache = FilterMetadataCache(
    repair_filter_lookup=FILTER_REPAIR_LOOKUP,
    repair_instrument_lookup=INSTRUMENT_REPAIR_LOOKUP,
    telescope='Euclid',
    cache={},
)
