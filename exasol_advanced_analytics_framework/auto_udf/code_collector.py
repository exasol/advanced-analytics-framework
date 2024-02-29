from typing import Any
import sys
import ast
import inspect
from collections import defaultdict
from itertools import chain
import re


def _get_object_declaration(obj_name_or_alias: str, module: Any) -> \
        tuple[str | None, Any | None]:
    """
    Find an object with a given name in the list of objects defined in the current
    module.
    If found returns
        - the module where the object is defined,
        - the object found,
    Otherwise, returns (None, None)
    """
    for name, obj in inspect.getmembers(module):
        if name == obj_name_or_alias:
            return inspect.getmodule(obj), obj
    return None, None


def _rename_object_in_code(source_code: str, object_name: str, new_object_name: str) -> str:
    """
    Finds all occurrences in code of the specified object name and replaces it with another
    name.
    """
    return re.sub(rf'\b{object_name}\b', new_object_name, source_code)


def _set_of_names_to_text(set_of_names: set[tuple[str, str]],
                          prefix: str, separator: str) -> str:
    return separator.join(
        f'{prefix}{name}' if name == alias  else f'{prefix}{name} as {alias}'
        for name, alias in set_of_names
    )


class CodeExtractor:

    def __init__(self, base_module_name: str, include_modules: str | None = None,
                 exclude_modules: str | None = None):

        self.base_module_name = base_module_name
        self.picked_objects: dict[str, Any] = {}
        self.include_pattern = None if include_modules is None else re.compile(include_modules)
        self.exclude_pattern = None if exclude_modules is None else re.compile(exclude_modules)
        # A list of top level blocks of code.
        self.code_blocks: list[str] = []
        # A set of modules to be imported as a whole, each represented by the tuple
        # (module_name, module_alias).
        self.mod_imports: set[tuple[str, str]] = set()
        # Objects to be imported individually. This is provided as a dictionary:
        # {'module_name': {'object_name1', 'object_name2', ...}}
        self.obj_imports: dict[str, set[tuple[str, str]]] = defaultdict(set)

    def _get_unique_name(self, obj_name: str) -> str:
        """
        Checks if the provided name is already associated with extracted object.
        If so, finds another name that will be unique.
        """
        suffix = 0
        unique_name = obj_name
        while unique_name in self.picked_objects:
            suffix += 1
            unique_name = f'{obj_name}_{suffix}'
        return unique_name

    def _get_module_process_level(self, ref_module_name: str) -> tuple[bool, bool]:
        """
        Returns two flags:
            An object can be imported from the specified module.
            An object shall be extracted from the specified module.
        """
        if self.exclude_pattern and self.exclude_pattern.match(ref_module_name):
            return False, False
        extractable = ((ref_module_name == self.base_module_name) or
                       (self.include_pattern and self.include_pattern.match(ref_module_name)))
        return True, extractable

    def _maybe_drill_down(self, ref_object: Any, ref_module: Any) -> str:

        if ref_object in self.picked_objects.values():
            # The object has already been extracted, just find its name.
            return next(obj_name for obj_name, obj_ref
                        in self.picked_objects.items() if obj_ref == ref_object)
        else:
            # Extract this object.
            return self.extract_object(ref_object, ref_module)

    def _process_attribute_node(self, source_code: str, root_module: Any,
                                namespace: str, attribute: str) -> str:
        """
        Processes a node of type ast.Attribute. This can be a reference to an
        object where the name (attribute) is qualified with either a class or
        module name or alias (namespace).

        Returns the code of the parent object, which can be modified.
        """

        ref_module, ref_object = _get_object_declaration(namespace, root_module)
        # We are looking for a case when the object of the namespace is the module.
        if (ref_module is not None) and (ref_object == ref_module):
            _, extractable = self._get_module_process_level(ref_module.__name__)
            if extractable:
                # Find the object of the attribute in the module of the namespace.
                _, ref_object = _get_object_declaration(attribute, ref_module)
                if ref_object is not None:
                    ref_object_name = self._maybe_drill_down(ref_object, ref_module)
                    # In the parent's code replace qualified references with unqualified.
                    q_name = f'{namespace}.{attribute}'
                    source_code = _rename_object_in_code(source_code, q_name, ref_object_name)

        return source_code

    def _process_name_node(self, source_code: str, root_module: Any, object_alias: str) -> str:
        """
        Processes a node of type ast.Name. This can be a reference to an object that should
        be extracted. All referenced objects whose name is not qualified with a class or a
        module name should be represented with this kind of node. Generally, the object name
        contained in the node shall be considered as an alias.

        Returns the code of the parent object, which can be modified.
        """

        # Get the module where this name is defined.
        ref_module, ref_object = _get_object_declaration(object_alias, root_module)
        if ref_module is not None:
            ref_module_name = ref_module.__name__
            importable, extractable = self._get_module_process_level(ref_module_name)
            if extractable:
                # Drill down unless the object is the module itself.
                if ref_object != ref_module:
                    ref_object_name = self._maybe_drill_down(ref_object, ref_module)
                    # If the object is imported with an alias then use the object name instead.
                    if ref_object_name != object_alias:
                        source_code = _rename_object_in_code(source_code, object_alias,
                                                             ref_object_name)

            elif importable and (ref_object == ref_module):
                # Remember the module import, e.g. "import module [as alias]"
                self.mod_imports.add((ref_module_name, object_alias))

            elif importable:
                # Remember the object import, e.g. "from module import object [as alias]"
                self.obj_imports[ref_module_name].add((ref_object.__name__, object_alias))

        elif ref_object is not None:
            # This must be a global variable.
            self.code_blocks.append(f'{object_alias} = {repr(ref_object)}')

        return source_code

    def _parse_object(self, root_object: Any) -> tuple[str, str]:
        """
        Parses the object and returns its name and the Python code.
        The name may get changed if it conflicts with a name of a
        previously extracted object.
        """

        source_code = inspect.getsource(root_object)

        # Check if the object's name will cause a name clash and rename it if necessary.
        root_object_name = root_object.__name__
        proposed_name = self._get_unique_name(root_object_name)
        if proposed_name != root_object_name:
            source_code = _rename_object_in_code(source_code, root_object_name, proposed_name)
            root_object_name = proposed_name

        return root_object_name, source_code

    def extract_object(self, root_object: Any, root_module: Any | None = None) -> str:
        """
        Traverses the code of the specified object and maybe some of the referenced
        objects.

        Collects the code of the traversed object. By default, the code of a referenced
        object is extracted only if the object is defined in the same module. The caller
        can specify which other modules the code should be extracted from.

        Parameters:
            root_object:        The root object to be traversed.
            root_module:        Optional module object where the root object is defined.
                                If not specified will get the module of the root object.

        Returns the name of the root object,
        """

        if root_module is None:
            root_module = inspect.getmodule(root_object)

        root_object_name, source_code = self._parse_object(root_object)

        self.picked_objects[root_object_name] = root_object

        # Walk the object tree looking for references to other object.
        tree = ast.parse(source_code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                source_code = self._process_attribute_node(source_code, root_module,
                                                           node.value.id, node.attr)
            elif isinstance(node, ast.Name):
                source_code = self._process_name_node(source_code, root_module, node.id)

        self.code_blocks.append(source_code)
        return root_object_name

    def get_complete_code(self) -> str:
        """
        Puts all extracted code blocks and import statements together and returns
        a complete Python code that can be executed.
        """

        # Convert the imports into single block of code and insert it into the list
        # of code blocks.
        imports = '\n'.join(
            f'from {module_name} import ({_set_of_names_to_text(obj_list, "", ", ")})'
            for module_name, obj_list in self.obj_imports.items()
        )
        imports = [_set_of_names_to_text(self.mod_imports, 'import ', '\n'),
                   imports]
        imports = '\n'.join(imp for imp in imports if imp)

        return '\n\n'.join(chain([imports], self.code_blocks) if imports
                           else self.code_blocks)
