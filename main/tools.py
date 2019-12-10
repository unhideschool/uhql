from functools import wraps


def logged_method(f):
    @wraps(f)
    def logged_method_decorator(*args, **kwargs):
        self: "UHQL" = args[0]

        # Do login
        self.d.login()

        # Run the method
        rv = f(*args, **kwargs)

        # Do logout
        self.d.logoff()
        return rv
    return logged_method_decorator
