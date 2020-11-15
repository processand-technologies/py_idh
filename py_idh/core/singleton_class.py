"""
example usage 

@Singleton
class DBConnection(object):

    def __init__(self):
        # Initialize your database connection here #
        self.logging_label = 'DBConnection'

    def __str__(self):
        return 'Database connection object'

warning: do not use static-/class- methods in singletons! (problem with decorators on static-/class- methods)
"""

class Singleton:
    __init_args = None
    __check_input = True

    def __init__(self, cls):
        self._cls = cls

    def Instance(self, args = None, check_input = True):
        if hasattr(self, '_instance'):
            if args and args != self.__init_args:
                raise Exception(f"""singleton instance received different input arguments as it was served:
                        1) {self.__init_args}, type '{type(self.__init_args)}'
                        2) {args}, type '{type(args)}' """)
            return self._instance
        else:
            self.__check_input = check_input
            if args:
                self._instance = self._cls(args)
                self.__init_args = args
            else:
                self._instance = self._cls()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)