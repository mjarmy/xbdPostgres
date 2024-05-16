//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestData
**
const class TestData
{
  new make()
  {
    recs := Ref:Dict[:]

    // extra
    n := 0
    extra := Dict[
      Etc.dict1("id", Ref.fromStr("z${n++}"))
    ]
    extra.each |d| { recs.add(d->id, d) }

    this.recs = recs
  }

  // Run a filter against the recs
  Dict[] filter(Filter f)
  {
    res := Dict[,]

    pather := |Ref r->Dict?| { recs.get(r) }
    recs.each |r|
    {
      if (f.matches(r, PatherContext(pather)))
        res.add(r)
    }

    return res
  }

  const Ref:Dict recs
}

