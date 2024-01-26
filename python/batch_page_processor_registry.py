registry = {}

def page_processor(page_processor_function):
    print("Moo!!!")
    registry[page_processor_function.__name__] = page_processor_function
    return page_processor_function

def list_page_processors():
    return list(registry.keys())