
def chunk_list(input_list, chunk_size):
    '''
    This function takes the input list and returns a list with adjacent groups of size
    chunk_list combined into tuples. The last tuple may be smaller than chunk_size.

    Example: input_list = [a, b, c, d, e, f, g], chunk_size = 3
        returns = [(a, b, c), (d, e, f), (g,)]
    '''
    '''
    for i in range(0, int(len(bin_id_list)/chunk_size + 2)):
        row_list = []
        for ci in range(0, chunk_size):
            idx = i*chunk_size + ci
            if idx >= len(bin_id_list):
                break
            row_list.append(bin_id_list[i*chunk_size + ci])
        chunked_bin_ids.append(tuple(row_list))
    '''
    output = []
    for i in range(0, int(len(input_list)/chunk_size + 2)):
        row_list = []
        for ci in range(0, chunk_size):
            idx = i*chunk_size + ci
            if idx >= len(input_list):
                break
            row_list.append(input_list[idx])
        if len(row_list) > 0:
            output.append(tuple(row_list))
    return output