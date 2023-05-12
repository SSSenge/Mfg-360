with open('auxFiles/bt.txt', 'w') as bt:
    r = ''
    for i in range(1, 1000):
        r += f'${i} varchar, '
    r += f'$1000 varchar'
    bt.write(r)