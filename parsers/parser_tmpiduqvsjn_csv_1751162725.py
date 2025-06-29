def parse(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    keys = lines[0].strip().split(',')
    data = []
    for line in lines[1:]:
        values = line.strip().split(',')
        data.append(dict(zip(keys, values)))
    return data