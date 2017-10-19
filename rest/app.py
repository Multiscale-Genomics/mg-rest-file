"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from __future__ import print_function

import os
import sys
#import logging

from flask import Flask, Response, request, make_response
from flask_restful import Api, Resource

from dmp import dmp
from reader.bigbed import bigbed_reader
from reader.bigwig import bigwig_reader
#from reader.tabix import tabix_reader
from reader.hdf5_reader import hdf5_reader

from rest.mg_auth import authorized

APP = Flask(__name__)
#logging.basicConfig()

def help_usage(error_message, status_code,
               parameters_required, parameters_provided):
    """
    Usage Help

    Description of the basic usage patterns for GET functions for the app,
    including any parameters that were provided byt he user along with the
    available parameters that are required/optional.

    Parameters
    ----------
    error_message : str | None
        Error message detailing what has gone wrong. If there are no errors then
        None should be passed.
    status_code : int
        HTTP status code.
    parameters_required : list
        List of the text names for each paramter required by the end point. An
        empty list should be provided if there are no parameters required
    parameters_provided : dict
        Dictionary of the parameters and the matching values provided by the
        user. An empyt dictionary should be passed if there were no parameters
        provided by the user.

    Returns
    -------
    str
        JSON formated status message to display to the user
    """
    parameters = {
        'by_user': ['By User [0|1]', 'int', 'OPTIONAL'],
        'file_id': ['File ID', 'str', 'REQUIRED'],
        'region': ['Chromosome:Start:End', 'str:int:int', 'OPTIONAL'],
        'file_type': ['File type (bb, bw, tsv, fasta, fastq, ...)', 'str', 'OPTIONAL'],
        'data_type': ['Data type (chip-seq, rna-seq, wgbs, ...)', 'str', 'OPTIONAL'],
        'assembly': ['Assembly', 'str', 'REQUIRED'],
        'chrom': ['Chromosome', 'str', 'OPTIONAL'],
        'start': ['Start', 'int', 'OPTIONAL'],
        'end': ['End', 'int', 'OPTIONAL'],
        'type': ['add_meta|remove_meta', 'str', 'OPTIONAL'],
        'output': [
            "Default is None. State 'original' to return the original whole file",
            'str', 'OPTIONAL'],
    }

    used_param = {k : parameters[k] for k in parameters_required if k in parameters}

    usage = {
        '_links' : {
            '_self' : request.base_url,
            '_parent' : request.url_root + 'mug/api/dmp'
        },
        'parameters' : used_param
    }
    message = {
        'usage' : usage,
        'status_code' : status_code
    }

    if parameters_provided:
        message['provided_parameters'] = parameters_provided

    if error_message is not None:
        message['error'] = error_message

    return message

def _get_dm_api(user_id=None):
    cnf_loc = os.path.dirname(os.path.abspath(__file__)) + '/mongodb.cnf'

    if user_id == 'test':
        print("TEST USER DM API")
        return dmp(cnf_loc, test=True)

    if os.path.isfile(cnf_loc) is True:
        print("LIVE DM API")
        return dmp(cnf_loc)

    print("TEST DM API")
    return dmp(cnf_loc, test=True)

class EndPoints(Resource):
    """
    Class to handle the http requests for returning information about the end
    points
    """

    def get(self):
        """
        GET list all end points

        List of all of the end points for the current service.

        Example
        -------
        .. code-block:: none
           :linenos:

           curl -X GET http://localhost:5002/mug/api/dmp/file

        """
        return {
            '_links': {
                '_self': request.base_url,
                '_getWholeFile': request.url_root + 'mug/api/dmp/file/whole',
                '_getFileRegion': request.url_root + 'mug/api/dmp/file/region',
                '_ping': request.url_root + 'mug/api/dmp/file/ping',
                '_parent': request.url_root + 'mug/api/dmp/file'
            }
        }


class File(Resource):
    """
    Class to handle the http requests for retrieving the data from a file.
    This class is able to handle big[Bed|Wig] file and serve back the matching
    region in the relevant format. It is also possible to stream back the whole
    file of any type for use in other tools.
    """

    @authorized
    def get(self, user_id):
        """
        GET  List values from the file

        Call to optain regions from the conpressed index files for Bed, Wig and
        TSV based file formats that contain genomic information.

        Other files can be streamed.

        Parameters
        ----------
        user_id : str
            User ID
        file_id : str
            Identifier of the file to retrieve data from
        region : str
            <chromosome>:<start_pos>:<end_pos>
        output : str
            Default is None. State 'original' to return the original whole file

        Returns
        -------
        file
            Returns a formated in the relevant file type with any genomic
            features matching the format of the file.

        Examples
        --------
        .. code-block:: none
           :linenos:

           curl -X GET http://localhost:5002/mug/api/dmp/file/whole?file_id=test_file

        """
        file_id = request.args.get('file_id')
        public = request.args.get('public')

        params = [file_id]

        # Display the parameters available
        if sum([x is None for x in params]) == len(params):
            return help_usage(None, 200, ['file_id'], {})

        if user_id is not None:
            selected_user_id = user_id['user_id']
            if public is not None:
                selected_user_id = user_id['public_id']

            dmp_api = _get_dm_api(selected_user_id)

            print(user_id, file_id)
            file_obj = dmp_api.get_file_by_id(selected_user_id, file_id)
            print(file_obj)

            if file_obj is None or 'file_path' not in file_obj:
                return help_usage('MissingFile', 400, ['file_id'], {})

            return Response(
                self._output_generate(file_obj['file_path']),
                mimetype='text/text'
            )

        return help_usage('Forbidden', 403, ['file_id'], {})

    def _output_generate(self, file_path):
        """
        Function to iterate through a file and stream it back to the user
        """
        if os.path.isfile(file_path):
            with open(file_path, 'rb') as f_strm:
                #for chunk in iter(lambda: f_strm.read(4096), b''):
                for chunk in iter(lambda: f_strm.read(64), b''):
                    yield chunk
        else:
            yield ""


class FileRegion(Resource):
    """
    Class to handle the http requests for retrieving the data from a file.
    This class is able to handle big[Bed|Wig] file and serve back the matching
    region in the relevant format. It is also possible to stream back the whole
    file of any type for use in other tools.
    """

    @authorized
    def get(self, user_id):
        """
        GET  List values from the file

        Call to optain regions from the conpressed index files for Bed, Wig and
        TSV based file formats that contain genomic information.

        Other files can be streamed.

        Parameters
        ----------
        user_id : str
            User ID
        file_id : str
            Identifier of the file to retrieve data from
        region : str
            <chromosome>:<start_pos>:<end_pos>

        Returns
        -------
        file
            Returns a formated in the relevant file type with any genomic
            features matching the format of the file.

        Examples
        --------
        .. code-block:: none
           :linenos:

           curl -X GET http://localhost:5002/mug/api/dmp/file/region?file_id=test_file&chrom=1&start=1000&end=2000

        """
        file_id = request.args.get('file_id')
        chrom = request.args.get('chrom')
        start = request.args.get('start')
        end = request.args.get('end')
        public = request.args.get('public')

        params = [file_id, chrom, start, end]

        # Display the parameters available
        if sum([x is None for x in params]) == len(params):
            return help_usage(None, 200, ['file_id', 'chrom', 'start', 'end'], {})

        # ERROR - one of the required parameters is NoneType
        if sum([x is not None for x in params]) != len(params):
            return help_usage('MissingParameters', 400, ['file_id', 'chrom', 'start', 'end'], {})

        if user_id is not None:
            selected_user_id = user_id['user_id']
            if public is not None:
                selected_user_id = user_id['public_id']

            dmp_api = _get_dm_api(selected_user_id)

            file_obj = dmp_api.get_file_by_id(selected_user_id, file_id, False)

            if file_obj is None or 'file_path' not in file_obj:
                return help_usage(
                    'MissingParameters', 400,
                    ['file_id', 'chrom', 'start', 'end'], {}
                )

            params = [file_id, chrom, start, end]

            output_str = ''
            if file_obj['file_type'] in ['bed', 'bb']:
                reader = bigbed_reader(user_id, file_obj['file_path'])
                output_str = reader.get_range(chrom, start, end, 'bed')
            elif file_obj['file_type'] in ['wig', 'bw']:
                print(chrom, start, end, 'wig')
                reader = bigwig_reader(user_id, file_obj['file_path'])
                output_str = reader.get_range(chrom, start, end, 'wig')
            # elif file_obj['file_type'] in ['gff3', 'tsv', 'tbi']:
            #     reader = tabix_reader(file_obj['file_path'])
            #     output_str = reader.get_range(chrom, start, end, 'gff3')

            resp = make_response(output_str, 'application/tsv')
            resp.headers["Content-Type"] = "text"

            return resp

        return help_usage('Forbidden', 403, ['file_id'], {})


class Ping(Resource):
    """
    Class to handle the http requests to ping a service
    """

    def get(self):
        """
        GET Status

        List the current status of the service along with the relevant
        information about the version.

        Example
        -------
        .. code-block:: none
           :linenos:

           curl -X GET http://localhost:5002/mug/api/dmp/ping

        """
        import rest.release as release
        res = {
            "status":  "ready",
            "version": release.__version__,
            "author":  release.__author__,
            "license": release.__license__,
            "name":    release.__rest_name__,
            "description": release.__description__,
            "_links" : {
                '_self' : request.base_url,
                '_parent' : request.url_root + 'mug/api/dmp'
            }
        }
        return res

#
# For the services where there needs to be an extra layer (adjacency lists),
# then there needs to be a way of forwarding for this. But the majority of
# things can be redirected to the raw files for use as a track.
#

sys._auth_meta_json = os.path.dirname(os.path.realpath(__file__)) + '/auth_meta.json'

# Define the URIs and their matching methods
REST_API = Api(APP)

#   List the available end points for this service
REST_API.add_resource(EndPoints, "/mug/api/dmp/file", endpoint='file_root')

#   Get the data for a specific track
REST_API.add_resource(FileRegion, "/mug/api/dmp/file/region", endpoint='region')

#   List the available species for which there are datasets available
REST_API.add_resource(File, "/mug/api/dmp/file/whole", endpoint='file')

#   Service ping
REST_API.add_resource(Ping, "/mug/api/dmp/file/ping", endpoint='ping')


# Initialise the server
if __name__ == "__main__":
    APP.run(port=5003, debug=True, use_reloader=False)
