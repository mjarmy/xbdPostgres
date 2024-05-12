//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestData contains the records from the 'Alpha' data set from Project
** Haystack, and from a Niagara station that Jason helped put together.
**
const class TestData
{
  new make()
  {
    recs := Ref:Dict[:]

    // alpha
    Grid alpha := JsonReader(File(`test_data/alpha.json`).in).readVal
    alpha.each |d, i|
    {
      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    // niagara
    f := File(`test_data/jason.txt`)
    f.eachLine |line|
    {
      Dict d := JsonReader(line.in).readVal

      if (recs.containsKey(d->id))
        throw Err("oops ${d->id}")
      recs.add(d->id, d)
    }

    this.recs = recs
  }

  Ref:Dict filter(Filter f)
  {
    result := Ref:Dict[:]

    pather := |Ref r->Dict?| { recs.get(r) }
    recs.each |rec, id|
    {
      if (f.matches(rec, PatherContext(pather)))
        result.add(id, rec)
    }

    return result
  }

  const Ref:Dict recs
}
