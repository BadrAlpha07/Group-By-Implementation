import os
import sys
import numpy.random as rd
import numpy as np

#defaults to generating groups size ~ 10%.
def gen_size(folder, mini, maxi, n):
    step = (maxi-mini)//n
    for i, fsize in enumerate(range(mini, maxi, step)):
        groups = rd.randint(1, maxi, 10) #get 10 random group values.
        with open(f"{folder}/{i}.csv", 'wb') as f:
            for j in range(fsize):
                f.write(str.encode(f"{j};{rd.choice(groups)}\n"))

    with open(f"{folder}/info.csv", "w") as f:
        f.write("\n".join(f"{i};{u}" for i, u in enumerate(range(mini, maxi, step))))


def gen_group(folder, size, n):
    
    for i,a in enumerate(np.linspace(0,1,n)):
        groups = rd.randint(1, size, (int(a*size)+1))
        #print(a)
        #print(int(a*size)+1)
        with open(f"{folder}/{i}.csv", 'wb') as f:
            for j in range(size):
                f.write(str.encode(f"{j};{rd.choice(groups)}\n"))

    with open(f"{folder}/info.csv","w") as f:
        f.write(f"input_size: {size}\n");
        f.write(f"n; groupsize / filesize") 
        f.write("\n".join(f"{i};{(int(u*size)+1) /size:.2f}" for i,u in enumerate(np.linspace(0,1,n)))) 


def main(n):
    os.makedirs("group", exist_ok = True)
    os.makedirs("size", exist_ok = True)
    gen_group("group",400000, n)
    gen_size("size",100, 1000000,n)




if __name__ == "__main__":
    # execute only if run as a script
    main(40)

