def list_to_bm(list):
    bm = 0
    for elm in list:
        bm = bm | (1 << elm)
    return bm

def bm_to_list(bm):
    list = []
    count = 0
    while bm != 0:
        if bm & 1:
            list.append(count)
        count += 1
        bm = bm >> 1
    return list

def list_of_list_to_bm_list(list):
    bm_list = []
    for elm in list:
        bm_list.append(list_to_bm(elm))

    return bm_list

def bm_list_to_list_of_list(bm_list):
    list_of_list = []
    for bm in bm_list:
        list_of_list.append(bm_to_list(bm))

    return list_of_list

def bm_in(elm, bm):
    if bm & (1 << elm):
        return True
    else:
        return False

def bm(elm):
    return (1 << elm)

def bm_insert(bm, elm):
    bm = bm | (1 << elm)
    return bm

def bm_rm(bm, elm):
    if bm_in(elm, bm):
        bm = bm ^ (1 << elm)
    return bm

def bm_is_subset(bm1, bm2):
    if (bm1 & bm2) ^ bm1 == 0:
        return True
    else:
        return False

def bm_intersection(bm1, bm2):
    return bm1 & bm2

