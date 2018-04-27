import sys
class Cluster:
    def __init__(self, init_member):
        self.members = [init_member[0]]
        self.representative = (init_member[1], init_member[2], init_member[3], init_member[4])

    def __add__(self, other):
        self.members += other.members
        self.representative = [(self.representative[i] + other.representative[i])/2 for i in range(len(self.representative))]
        return self

    def __str__(self):
        return "Cluster Members: {}, Centroid: ({},{},{},{})".format(self.members,self.representative[0], self.representative[1],
                                                                 self.representative[2],self.representative[3])


def closestpair(L):
    def square(x):
        return x * x

    def sqdist(p, q):
        return sum([square(p[i] - q[i]) for i in range(len(p) - 2)])

    best = [sqdist(L[0], L[1]), (L[0], L[1])]

    def testpair(p, q):
        d = sqdist(p, q)
        if d < best[0]:
            best[0] = d
            best[1] = p, q

    def merge(A, B):
        i = 0
        j = 0
        while i < len(A) or j < len(B):
            if j >= len(B) or (i < len(A) and A[i][1] <= B[j][1]):
                yield A[i]
                i += 1
            else:
                yield B[j]
                j += 1

    def recur(L):
        if len(L) < 2:
            return L
        split = len(L) / 2
        splitx = L[split][0]
        L = list(merge(recur(L[:split]), recur(L[split:])))

        E = [p for p in L if abs(p[0] - splitx) < best[0]]
        for i in range(len(E)):
            for j in range(1, 8):
                if i + j < len(E):
                    testpair(E[i], E[i + j])
        return L

    L.sort()
    recur(L)
    return best[1][0][-1], best[1][1][-1]

def merge_Clusters(clusters, idx, data):
    l = 4
    clusters[idx[0]][-2].extend(clusters[idx[1]][-2])
    sumL = [0]*l
    for ID in clusters[idx[0]][-2]:
        sumL = [sumL[i] + data[ID][i] for i in range(4)]
    for i in range(4):
        clusters[idx[0]][i] = float(sumL[i])/l
    del clusters[idx[1]]
    return clusters



# returns: (7,6),(5,8)


if __name__ == "__main__":
    k = int(sys.argv[2])
    print(sys.argv[1])
    data = dict()
    cluster_list = []
    clusters = []
    ID = 0
    with open(sys.argv[1], 'r') as dataFile:
        for line in dataFile:
             line = line.split(',')
             try:
                data[ID] = (float(line[0]), float(line[1]), float(line[2]), float(line[3]), line[4].strip(), ID)
                clusters.append( [float(line[0]), float(line[1]), float(line[2]), float(line[3]), [ID], ID] )
                cluster_list.append(Cluster((ID,float(line[0]), float(line[1]), float(line[2]), float(line[3]))))
                ID += 1
             except: pass

    while len(clusters) > k:
        idxp, idxq = closestpair(clusters)
        i = 0
        found = False
        idx = []
        while (len(idx)<2) and (i < len(clusters)):
            if clusters[i][-1] == idxp or clusters[i][-1] == idxq:
                idx.append(i)
            i += 1
        idx.sort()
        clusters = merge_Clusters(clusters, idx, data)
    i = 0
    C = dict()
    for c in clusters:
        #print("cluster: {}".format(i))
        i += 1
        lst = []
        for ID in c[4]:
            lst.append([data[ID][0], data[ID][1], data[ID][2], data[ID][3], data[ID][4]])
            #print("[{}, {}, {}, {}, {}]".format( data[ID][0], data[ID][1], data[ID][2], data[ID][3], data[ID][4]) )
        C[i] = lst

    clusterDict = dict()
    wrongCount = 0
    for key, item in C.items():
        #dct[item[4][4]]
        dct = dict()
        for el in item:
            if el[4] in dct:
                dct[el[4]] += 1
            else:
                dct[el[4]] = 1
        clusterDict[max(dct, key = dct.get)] = item
        wrongCount += sum( [ value for key, value in dct.items() if key !=  max(dct, key = dct.get)] )

    f = open("results.txt",'w')
    for key,item in clusterDict.items():
        item.sort(key=lambda x: x[4])
        print("Cluster: {}".format(key))
        f.write("Cluster: {}\n".format(key))
        for el in item:
            print("[{}, {}, {}, {}, {}]".format(el[0], el[1], el[2], el[3], el[4]))
            f.write("[{}, {}, {}, {}, {}]\n".format(el[0], el[1], el[2], el[3], el[4]))
        print("Number of points in this cluster:{}".format(len(item)))
        print('\n')
        f.write("Number of points in this cluster:{}\n\n".format(len(item)))
    print("Number of points wrongly assigned:{}".format(wrongCount))
    f.write("Number of points wrongly assigned:{}".format(wrongCount))
    f.close()






