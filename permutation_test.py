#!/usr/bin/python
"""Permute rows of matrix before dispatching. On computation completion, generate report.
"""
def permute_rows(M, seed=True):
  """Randomly permute values in rows of M. Preserve masks. Permutes in place."""
  if seed:
    random.seed()
  for i in xrange(np.size(M,0)):
    try:
      M.mask
    except AttributeError:
      random.shuffle(M[i,:])
    else:
      n_masked = np.sum(M[i,:].mask)
      row = M[i,:].compressed()
      assert np.sum(np.isnan(row)) == 0
      row = list(row)
      row.extend([np.nan]*n_masked)
      random.shuffle(row)
      M[i,:] = row
      M[i,:].mask = np.isnan(row)


if __name__ == "__main__":
  main()
