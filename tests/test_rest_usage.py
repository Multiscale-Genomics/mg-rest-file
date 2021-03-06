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
import tempfile
import json
import pytest

from context import app

@pytest.fixture
def client(request):
    """
    Definges the client object to make requests against
    """
    db_fd, app.APP.config['DATABASE'] = tempfile.mkstemp()
    app.APP.config['TESTING'] = True
    client = app.APP.test_client()

    def teardown():
        """
        Close the client once testing has completed
        """
        os.close(db_fd)
        os.unlink(app.APP.config['DATABASE'])
    request.addfinalizer(teardown)

    return client

def test_ping(client):
    """
    Test that the ping function is returning the status key
    """
    rest_value = client.get('/mug/api/dmp/file/ping')
    details = json.loads(rest_value.data)
    #print(details)
    assert 'status' in details

def test_region(client):
    """
    Test that the track endpoint is returning the usage paramerts
    """
    rest_value = client.get(
        '/mug/api/dmp/file/region',
        headers=dict(Authorization='Authorization: Bearer teststring')
    )
    details = json.loads(rest_value.data)
    # print(details)
    assert 'usage' in details

def test_whole(client):
    """
    Test that the track endpoint is returning the usage paramerts
    """
    rest_value = client.get(
        '/mug/api/dmp/file/whole',
        headers=dict(Authorization='Authorization: Bearer teststring')
    )
    details = json.loads(rest_value.data)
    # print(details)
    assert 'usage' in details
