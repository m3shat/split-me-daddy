from audioop import reverse
import sys, getopt, os, json, hashlib, shutil, uuid

#overhead for each file is configured for 1KB
file_overhead = 1000
dryrun = False

def main(argv):

    destinations = None
    sources = None
    catalogue_file = None

    try:
        opts, args = getopt.getopt(argv,"hi:o:c:n",["sources=","destinations=","catalogue="])
    except getopt.GetoptError:
        print ("script.py -i <source files> -o <backup destinations> -c <catalogue file>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("script.py -i <source files> -o <backup destinations> -c <catalogue file>")
            sys.exit()
        elif opt in ("-o", "--destinations"):
            destinations = arg.split(",")
        elif opt in ("-i", "--sources"):
            sources = arg.split(",")
        elif opt in ("-c", "--catalogue"):
            catalogue_file = arg
        elif opt in ("-n"):
            globals()['dryrun'] = True

    discovered_destinations = discover_destinations(destinations)
    filelist = discover_sources(sources)

    print("loading catalogue...")
    catalogue = create_or_load_catalouge(catalogue_file)

    provisioned_filelist = provision_files(filelist, discovered_destinations, catalogue)

    copy_files(provisioned_filelist, discovered_destinations, catalogue, catalogue_file)

    print("saving catalogue...")
    save_catalogue(catalogue, catalogue_file)

    #print(discovered_destinations)


def provision_files(filelist, destinations, catalogue):

    provisioned_filelist = []

    # order filelist by size, provision large files first
    filelist.sort(key=lambda x: x['size'], reverse=True)

    l = len(filelist) + 1
    count = 1
    printProgressBar(0, l, prefix = 'Assign:', suffix = 'Complete')

    cat_dict = {}
    for cat_item in catalogue['files']:
        cat_dict[cat_item['id']] = cat_item

    for file in filelist:
        printProgressBar(count + 1, l, prefix = 'Assign:', suffix = 'Complete')
        count = count + 1
        #found = False
        #for catalogue_item in catalogue['files']:
        #    if catalogue_item['id'] == file['id']:
        #        found = True
        #        break
        if file['id'] in cat_dict:
            continue
        else:
            provisioned = False
            for destination in destinations:
                # check if file is small enough for destination
                if (destination['free_calculated'] - (file['size'] + file_overhead)) > 0:
                    # assign file to destination
                    file_metadata = file
                    file_metadata['backup'] = {
                        'destination': destination['id'],
                        'path': file['relative_full_path']
                    }
                    provisioned_filelist.append(file_metadata)
                    destination['free_calculated'] = destination['free_calculated'] - (file['size'] + file_overhead)
                    provisioned = True
                    break;
            if provisioned == False:
                print("Out of disk space.")
                sys.exit(5)

    return provisioned_filelist

def copy_files(provisioned_filelist, discovered_destinations, catalogue, catalogue_file):
    try:
        transfered = 1
        total = 0
        for item in provisioned_filelist:
            total = total + item['size']

        printProgressBar(transfered, total+1, prefix = 'Copy:', suffix = 'Complete')
        for file in provisioned_filelist:
            src = os.path.join(file['path'], file['name'])

            destination_path = None
            for disc_dest in discovered_destinations:
                if disc_dest['id'] == file['backup']['destination']:
                    destination_path = disc_dest['path']
                    break
            dst = os.path.join(destination_path, file['backup']['path'])

            if not dryrun:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copyfile(src, dst)

            catalogue['files'].append(file)

            transfered = transfered + file['size']
            printProgressBar(transfered, total, prefix = 'Copy:', suffix = 'Complete')

        if total == 0:
            printProgressBar(1, 1, prefix = 'Copy:', suffix = 'Complete')

    except KeyboardInterrupt:
        print("Interrupted, saving catalogue...")
        save_catalogue(catalogue, catalogue_file)
        sys.exit()


def discover_destinations(destinations):
    discovered_destinations = []

    for destination in destinations:
        total, used, free = shutil.disk_usage(destination)
        print("Destination " + destination + ": ")
        print("Total: %d GiB" % (total // (2**30)))
        print("Used: %d GiB" % (used // (2**30)))
        print("Free: %d GiB" % (free // (2**30)))

        dest = {
            'id': read_or_create_destination_id(destination),
            'path': destination,
            'size': total,
            'free': free,
            'free_calculated': free
        }

        discovered_destinations.append(dest)
    
    return discovered_destinations

# Looks through source items and generates a list of files
def discover_sources(sources):
    discovered_files = []

    for source in sources:
        for root, dirs, files in os.walk(source):
            for file in files:
                stat = os.stat(os.path.join(root,file))
                file_obj = {
                    'id': generate_file_id(file, root, stat.st_size, stat.st_mtime),
                    'name': file,
                    'path': root,
                    'relative_full_path': os.path.relpath(os.path.join(root, file), source),
                    'size': stat.st_size,
                    'modified_time': stat.st_mtime
                }
                discovered_files.append(file_obj)
    
    return discovered_files

def generate_file_id(name, path, size, modified):
    return hashlib.sha384((path + name + str(size) + str(modified)).encode('utf-8')).hexdigest()

def save_catalogue(data, catalogue_file):
    with open(catalogue_file, 'w') as outfile:
        outfile.write(json.dumps(data))

# read catalogue file to memory
def load_catalogue(catalogue_file):
    with open(catalogue_file, "r") as json_file:
        data = json.load(json_file)
        return data

def create_catalogue(catalogue_file):
    initial_catalogue = {
        'metadata': {},
        'files': []
    }
    save_catalogue(initial_catalogue, catalogue_file)
    return initial_catalogue
    

def create_or_load_catalouge(catalogue_file):
    try:
        return load_catalogue(catalogue_file)
    except FileNotFoundError:
        return create_catalogue(catalogue_file)

def read_destination_id(destination):
    with open(os.path.join(destination,".dest_id"), "r") as file:
        return file.read()

def create_destination_id(destination):
    destination_id = str(uuid.uuid1());
    with open(os.path.join(destination,".dest_id"), "w") as file:
        file.write(str(destination_id))
    return destination_id

def read_or_create_destination_id(destination):
    try:
        return read_destination_id(destination)
    except FileNotFoundError:
        return create_destination_id(destination)

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()

if __name__ == "__main__":
    main(sys.argv[1:])
