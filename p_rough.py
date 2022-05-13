if __name__ == '__main__':

    derived_column_map = {}
    with open("global_mappings.txt") as f:
        for line in f:
            (key, val) = line.strip().split(':')
            derived_column_map[key] = val.strip().split(',')

    for key in derived_column_map:
        aer = 'jar_material_colour_mvqq'
        print('key', key)
        val_list = derived_column_map.get(key)
        for val in val_list:
            if aer in val:
                print('true')
            else:print('false')