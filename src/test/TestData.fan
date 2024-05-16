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

    // extra
    n := 0
    Dict[] extra := JsonReader(
      """[
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 1, "b": 1 },
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 2, "b": 2 },
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 3, "b": 3 },
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 4, "b": 4 },
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 5, "c": { "d": 1 }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 6, "c": { "d": 2 }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 7, "c": { "d": 3 }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "a": 8, "c": { "d": 4 }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "e": true},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "f": { "_kind": "uri", "val": "https://project-haystack.org" }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "g": { "_kind": "coord", "lat": 50.979603, "lng": 10.318789 }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "h": { "_kind": "date", "val": "2021-03-22" }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "h": { "_kind": "date", "val": "2021-03-23" }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "i": { "_kind": "time", "val": "17:19:23" }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "i": { "_kind": "time", "val": "17:19:24" }},
         { "id": { "_kind": "ref", "val": "x${n++}" }, "j": { "_kind": "dateTime", "val": "2021-03-22T13:57:00.381-04:00", "tz": "New_York" } },
         { "id": { "_kind": "ref", "val": "x${n++}" }, "j": { "_kind": "dateTime", "val": "2021-03-22T13:57:00.382-04:00", "tz": "New_York" } }
         ]""".in).readVal
    extra.each |d| { recs.add(d->id, d) }

    // done
    this.recs = recs

    // make the records queryable
    queryable := Ref:Dict[:]
    recs.each |v,k| { queryable.add(k, makeQueryable(v)) }
    this.queryable = queryable
  }

  // Run a filter against the queryable recs
  Dict[] filter(Filter f)
  {
    result := Dict[,]

    pather := |Ref r->Dict?| { queryable.get(r) }
    queryable.each |rec, id|
    {
      q := queryable.get(id)
      if (f.matches(q, PatherContext(pather)))
        result.add(recs.get(id))
    }

    return result
  }

  // Make a rec queryable by stripping the units from numbers
  private static Dict makeQueryable(Dict d)
  {
    values := Str:Obj[:]
    d.each |v,k|
    {
      // traverseDict nested dict
      if (v is Dict)
      {
        values.add(k, makeQueryable(v))
      }
      // Number
      else if (v is Number)
      {
        n := (Number) v

        // Strip units
        if (n.unit != null)
        {
          n = n.isInt ?
            Number.makeInt(n.toInt) :
            Number(n.toFloat)
        }
        values.add(k, n)
      }
      // anything else
      else
      {
        values.add(k, v)
      }
    }
    return Etc.makeDict(values)
  }

  const Ref:Dict recs
  const Ref:Dict queryable
}

