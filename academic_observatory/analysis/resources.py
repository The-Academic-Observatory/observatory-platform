from abc import ABC, abstractmethod


class AbstractObservatoryResource(ABC):

    def __init__(self, df):
        self.df = df

    def save_image(self, name, kwargs):
        raise NotImplementedError

    def save_gcp_bucket(self, name, bucket, kwargs):
        raise NotImplementedError
