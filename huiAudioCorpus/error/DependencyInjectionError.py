from typing import Dict


class DependencyInjectionError(Exception):

    def __init__(self, exception: Exception, class_config: Dict[str,str] , class_name: str, requested_class_name: str):
        self.exception = exception
        self.class_config = class_config
        self.class_name = class_name
        self.requested_class_name = requested_class_name

        super().__init__(f'Dependent object {self.class_name} could not be injected for {self.requested_class_name}')

    def __str__(self):
        return self.get_string()

    def get_string(self):
        string = f'\n+++++++++++++++++++++++++\n'
        string += 'Error during creation of dependencies. Maybe your config is wrong. \n'
        string += f'Dependent object "{self.class_name}" could not be injected for "{self.requested_class_name}" \n'
        string += f'with error message: {self.exception} \n'
        string += f'config parameter used are: {self.class_config}\n'
        string += f'+++++++++++++++++++++++++\n'
        return string
