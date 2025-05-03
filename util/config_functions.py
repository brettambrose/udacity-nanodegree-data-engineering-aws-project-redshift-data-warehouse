import configparser

def modify_config_file(config_file: str,
                       config_obj: object,
                       config_section: str,
                       config_key: str,
                       config_val: str
                       ):
    """
    Overwrites sections, keys, and/or values in a config file at runtime.
    Example use case: a Redshift cluster is created and deployed at runtime, and the
     cluster's endpoint needs to be added to a config file for later user in the 
     application.  This function will write the endpoint into a config file.

    Args
     config_file (string): path and name of config file being modified
     config_obj (object): configparser.ConfigParser() class for the config file
     config_section (string): section being added/modified in config file
     config_key (string): key under config_section being added/modified
     config_val (string): config_key value being added/modifed
    """

    try:
        config_obj[config_section]

        print(f"{config_section} section exists in {config_file}")

        try:
            val = config_obj.get(config_section, config_key)

            if len(val) == 0 or val != config_val:
                config_obj_override = configparser.ConfigParser()
                config_obj_override.read(config_file)

                config_obj_override[config_section][config_key] = config_val

                with open(config_file, "w") as configfile:
                    config_obj_override.write(configfile)

                config_obj.read(config_file)

                print(f"{config_key} reconfigured for {config_section}")

            else:
                print(f"{config_key} already configured for {config_section}")
                
        except Exception as e:
            print(e)
            
    except:
        try:
            print(f"Adding {config_section} section with {config_key} key to {config_file}")
            
            config_obj_override = configparser.ConfigParser()
            config_obj_override.read(config_file)

            config_obj_override[config_section] = {config_key: config_val}

            with open(config_file, "w") as configfile:
                config_obj_override.write(configfile)

            config_obj.read(config_file)

            print(f"{config_section} section with {config_key} added to {config_file}")

        except Exception as e:
            print(e)
            