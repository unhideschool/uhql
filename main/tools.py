from functools import wraps


def logged_method(f):
    @wraps(f)
    def logged_method_decorator(*args, **kwargs):
        self: "UHQL" = args[0]

        self.d.login()
        if self.d.can(kwargs["jsonrequest"]):
            rv = f(*args, **kwargs)
        else:
            raise Exception("UNAUTHORIZED")

        self.d.logoff()
        return rv
    return logged_method_decorator
