from configparser import ConfigParser
from fileinput import filename

# config = configparser.ConfigParser()
# config.read('db.ini')


#print(config['postgresql']['host'])

# def config(filename='db.ini', section='postgresql'):
def config(filename='db.ini',section='default'):
    # create a parser
    
    dbparser = ConfigParser()
    # read config file
    dbparser.read(filename)

    # get section, default to postgresql
    db = {}
    if dbparser.has_section(section):
        params = dbparser.items(section)
        section1 = params[0][1]

        if dbparser.has_section(section1):
            params = dbparser.items(section1)
    
            for param in params:
                db[param[0]] = param[1]
            db[section] = section1
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db
