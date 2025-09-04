import re
import yaml

class MACAddressYamlHandler:
    """Encapsulates handling of MAC addresses in YAML."""
    _MAC_PATTERN = re.compile(r'^(?:[0-9A-Fa-f]{2}[:-]){5}(?:[0-9A-Fa-f]{2})$')
    
    @classmethod
    def create_loader(cls):
        """Create a YAML loader that treats MAC addresses as strings."""
        class _CustomLoader(yaml.SafeLoader):
            pass

    
        # Add MAC address resolver with higher priority
        mac_first_chars = list('0123456789ABCDEFabcdef')
    
        for first_char in mac_first_chars:
            if first_char not in _CustomLoader.yaml_implicit_resolvers:
                _CustomLoader.yaml_implicit_resolvers[first_char] = []
            
            # Insert MAC resolver at the beginning (highest priority)
            _CustomLoader.yaml_implicit_resolvers[first_char].insert(0, 
                ('tag:yaml.org,2002:str', cls._MAC_PATTERN))
        return _CustomLoader
    
    @classmethod
    def create_dumper(cls):
        """Create a YAML dumper that outputs MAC addresses without quotes."""
        class _CustomDumper(yaml.SafeDumper):
            def choose_scalar_style(self):
                # Check if this is a MAC address and force plain style
                if hasattr(self, 'event') and hasattr(self.event, 'value') and cls._MAC_PATTERN.match(self.event.value):
                    self.event.implicit = (True, self.event.implicit[1])
                    return ''  # Force plain style for MAC addresses
                # Use the original logic for non-MAC addresses
                return super().choose_scalar_style()
        
        _CustomDumper.add_representer(str, _CustomDumper.represent_str)
        return _CustomDumper

# Usage - replace global variables
CustomYamlLoader = MACAddressYamlHandler.create_loader()
CustomYamlDumper = MACAddressYamlHandler.create_dumper()