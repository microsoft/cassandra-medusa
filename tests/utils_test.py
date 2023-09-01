# -*- coding: utf-8 -*-
# Copyright 2021 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

import medusa.utils


class RestoreNodeTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_null_if_empty(self):
        assert medusa.utils.null_if_empty("") is None
        assert medusa.utils.null_if_empty(None) is None
        assert medusa.utils.null_if_empty("test") == "test"
        assert medusa.utils.null_if_empty(1) == 1

    def test_append_suffix(self):
        file_name = "cassandra-westus2000000/data/system_schema/views-9786ac1cdd583201a7cdad556410c985/nb-1-big-Data.db"
        appended_file_name = medusa.utils.append_suffix(file_name)
        assert appended_file_name.startswith(file_name)
        assert len(appended_file_name) == len(file_name) + 32

    def test_remove_suffix(self):
        file_name = "cassandra-westus2000000/data/system_schema/views-9786ac1cdd583201a7cdad556410c985/nb-1-big-Index.db3c9baf37d5eca8bb2c3f29da1eabb7ab"
        removed_file_name = medusa.utils.remove_suffix(file_name)
        assert removed_file_name == "cassandra-westus2000000/data/system_schema/views-9786ac1cdd583201a7cdad556410c985/nb-1-big-Index.db"

        file_name = "cassandra-westus2000000/data/system_schema/views-9786ac1cdd583201a7cdad556410c985/nb-1-big-Index.db"
        removed_file_name = medusa.utils.remove_suffix(file_name)
        assert removed_file_name == "cassandra-westus2000000/data/system_schema/views-9786ac1cdd583201a7cdad556410c985/nb-1-big-Index.db"

if __name__ == '__main__':
    unittest.main()
