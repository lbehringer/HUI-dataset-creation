class_highlighter = '###'
end_of_class = '____'

class ToString:
    def __str__(self):
        return ModelToStringConverter().convert(self)  # pragma: no cover

class ModelToStringConverter:
    def convert(self, model):
        strings = []
        strings.append(self.get_class_text(model))
        strings.append('')
        attributes = self.get_all_attributes(model)
        [strings.append(self.get_method_text(model, attr)) for attr in attributes]
        strings.append(end_of_class)
        string = '\n'.join(strings)
        return string

    def get_class_text(self, model):
        string = class_highlighter + ' ' + model.__class__.__name__ + ' ' + class_highlighter
        return string

    def get_all_attributes(self, model):
        all_attributes = dir(model)
        all_attributes = [attr for attr in all_attributes if not attr.startswith('__')]
        return all_attributes

    def get_method_text(self, model, method_name: str):
        value = getattr(model, method_name)
        value_string = self.get_value_text(value)
        string = method_name + ' ' + str(type(value)) + ': ' + value_string
        return string

    def get_value_text(self, value):
        if isinstance(value, float):
            return str(round(value, 2))

        string = str(value)
        if len(string) > 20:
            return string[:20] + ' ...'
        return str(value)
