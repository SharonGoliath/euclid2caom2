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
#  : 4 $
#
# ***********************************************************************
#


from caom2utils.parsers import BlueprintParser, FitsParser
from caom2pipe import caom_composable as cc
from euclid2caom2 import main_app


__all__ = ['EUCLIDFits2caom2Visitor']


class EUCLIDFits2caom2Visitor(cc.Fits2caom2VisitorRunnerMeta):
    def __init__(self, observation, **kwargs):
        super().__init__(observation, **kwargs)

    def _get_mapping(self, dest_uri):
        if self._storage_name.is_auxiliary():
            return main_app.EUCLIDMappingAuxiliary(
                self._clients,
                self._config,
                dest_uri,
                self._observation,
                self._reporter,
                self._storage_name,
            )
        else:
            if '-NIR-' in dest_uri:
                return main_app.EUCLIDMappingNIR(
                    self._clients,
                    self._config,
                    dest_uri,
                    self._observation,
                    self._reporter,
                    self._storage_name,
                )
            else:
                return main_app.EUCLIDMappingVIS(
                    self._clients,
                    self._config,
                    dest_uri,
                    self._observation,
                    self._reporter,
                    self._storage_name,
                )

    def _get_parser(self, blueprint, uri):
        headers = self._storage_name.metadata.get(uri)
        if headers is None or len(headers) == 0 or self._storage_name.is_auxiliary():
            parser = BlueprintParser(blueprint, uri)
        else:
            parser = FitsParser(headers, blueprint, uri)
        self._logger.debug(f'Created {parser.__class__.__name__} parser for {uri}.')
        return parser


def visit(observation, **kwargs):
    return EUCLIDFits2caom2Visitor(observation, **kwargs).visit()
