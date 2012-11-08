import random
import numpy as np

def permute_rows(M):
  """Randomly permute values in rows of numpy matrix M. Preserve masks."""
  random.seed()
  for i in xrange(np.size(M,0)):
    random.shuffle(M[i,:])

def rank_rows(M):
  """Rank order rows of M. Preserve masks."""
  random.seed()
  for i in xrange(np.size(M,0)):
    try:
      mask = M.mask
    except AttributeError:
      M[i,:] = M[i,:].argsort().argsort()
    else:
      n_values = np.sum(M[i,:].mask)
      M[i,:].data = M[i,:].argsort().argsort()
      # Mask any rank over the total number of values
      M[i,:].mask = (M[i,:].data >= n_values)
