select 
  id, 
  --paths, 
  --refs,
  --strs,
  --nums,
  --units,
  --bools,
  --uris
  dates,
  times,
  dateTimes
from rec where (paths @> '{"haven"}'::text[]);

