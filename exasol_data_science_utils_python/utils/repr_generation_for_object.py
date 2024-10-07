def generate_repr_for_object(obj):
    parameters = ",".join(f"{k}: {v}" for k, v in obj.__dict__.items())
    return f"{obj.__class__.__name__}({parameters})"