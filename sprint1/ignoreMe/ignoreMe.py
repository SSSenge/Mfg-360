hw = {'H': [['e','l'],['l','o'],['w','o'],['r','l'],['d','!']]}

print(f"{[k for k in hw.keys()][0]}{''.join(list(map(lambda x: (f'{x:>2s}') if x == [v for v in hw.values()][0][2][0] else x,''.join(list(map(lambda x: x[0] + x[1],[v for v in hw.values()][0]))))))}")


# x if x != (v2 for v2 in hw.values() if v2 == hw.values()[0][2][0]) else [x].append(x)
# print([v for v in hw.values()][0][2][0])