# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Tuan Chien


from abc import ABC, abstractmethod

class DataQualityAnalyser(ABC):
    """ Data Quality Analyser Interface """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'run') and callable(subclass.run) and
                hasattr(subclass, 'erase') and callable(subclass.erase)
                or NotImplemented)

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the analyser.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """
        raise NotImplementedError

    @abstractmethod
    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by all modules in the analyser.
        @param index: If index=True, will also delete indices.
        @param kwargs: Unused.
        """

        raise NotImplementedError


class MagAnalyserModule(ABC):
    """
    Analysis module interface for the MAG analyser.
    """

    @classmethod
    def __subclasshook__(cls, subclass):
        return (hasattr(subclass, 'run') and callable(subclass.run) and
                hasattr(subclass, 'name') and callable(subclass.name) and
                hasattr(subclass, 'erase') and callable(subclass.erase)
                or NotImplemented)

    @abstractmethod
    def run(self, **kwargs):
        """
        Run the analyser.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """

        raise NotImplementedError

    @abstractmethod
    def erase(self, index: bool = False, **kwargs):
        """
        Erase elastic search records used by the module and possibly delete the index.
        @param index: If index=True, will also delete indices.
        @param kwargs: Optional key value arguments to pass into an analyser. See individual analyser documentation.
        """

        raise NotImplementedError

    def name(self) -> str:
        """ Get the name of the module.
        @return: Name of the module.
        """

        return self.__class__.__name__
