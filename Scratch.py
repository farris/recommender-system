
#%%
import itertools

params = [ [0.1, 0.99],    [0.001, 0.01],        [1, 2]         ] 
params = list(itertools.product(*params))
for i in params:
    print(i)
# %%
