'''
This file is part of the PSL software.
Copyright 2011-2015 University of Maryland
Copyright 2013-2018 The Regents of the University of California

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import tests.data.models.simpleacquaintances
from tests.base_test import PSLTest

class TestModel(PSLTest):
    def test_simple_acquaintances(self):
        results = tests.data.models.simpleacquaintances.run()

        self.assertEquals(len(results), 1)

        predicate, frame = list(results.items())[0]
        self.assertEquals(predicate.name(), 'KNOWS')
        self.assertEquals(len(frame), 52)
