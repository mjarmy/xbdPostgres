//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Rec is a record in the postgres 'rec' table.
**
internal const class Rec
{
  **
  ** Only used for unit tests
  **
  internal new make(
    Str       id,
    Str[]     paths,
    Str:Str   refs,
    Str:Str   strs,
    Str:Float nums,
    Str:Str   units,
    Str:Bool  bools,
    Str:Str   uris,
    Str:Str   dates,
    Str:Str   times,
    Str:Int   dateTimes)
  {
    this.id    = id
    this.paths = paths
    this.refs  = refs
    this.strs  = strs
    this.nums  = nums
    this.units = units
    this.bools = bools
    this.uris  = uris
    this.dates = dates
    this.times = times
    this.dateTimes = dateTimes
  }

  **
  ** Create a Rec by transforming a top-level Dict
  **
  internal static new fromDict(Dict dict)
  {
    paths := Str[,]
    refs  := Str:Str[:]
    strs  := Str:Str[:]
    nums  := Str:Float[:]
    units := Str:Str[:]
    bools := Str:Bool[:]
    uris  := Str:Str[:]
    dates := Str:Str[:]
    times := Str:Str[:]
    dateTimes := Str:Int[:]

    traverseDict(
      dict, Str[,], paths,
      refs, strs, nums, units, bools, uris,
      dates, times, dateTimes)

    return Rec(
      dict.id.id, paths,
      refs, strs, nums, units, bools, uris,
      dates, times, dateTimes)
  }

  private static Void traverseDict(
      Dict d,
      Str[] curPath,
      Str[] paths,
      Str:Str refs,
      Str:Str strs,
      Str:Float nums,
      Str:Str units,
      Str:Bool bools,
      Str:Str uris,
      Str:Str dates,
      Str:Str times,
      Str:Int dateTimes)
  {
    d.each |val, key|
    {
      curPath.add(key)
      dotted := curPath.join(".")
      paths.add(dotted)

      // dict
      if (val is Dict)
      {
        traverseDict(
          val, curPath, paths,
          refs, strs, nums, units, bools, uris,
          dates, times, dateTimes)
      }
      // Ref
      else if (val is Ref)
      {
        refs.add(dotted, ((Ref) val).id)
      }
      // Str
      else if (val is Str)
      {
        strs.add(dotted, val)
      }
      // Number
      else if (val is Number)
      {
        Number n := (Number) val
        nums.add(dotted, n.toFloat)
        units.add(dotted, n.unit == null ? "_" : n.unit.toStr)
      }
      // Bool
      else if (val is Bool)
      {
        bools.add(dotted, val)
      }
      // Uri
      else if (val is Uri)
      {
        uris.add(dotted, ((Uri) val).toStr)
      }
      // Date
      else if (val is Date)
      {
        dates.add(dotted, ((Date) val).toStr)
      }
      // Time
      else if (val is Time)
      {
        times.add(dotted, ((Time) val).toStr)
      }
      // DateTime
      else if (val is DateTime)
      {
        DateTime ts := (DateTime) val

        dateTimes.add(dotted, Duration(ts.ticks).toMillis)

        // for ->date and ->time
        datePath := dotted + ".date"
        timePath := dotted + ".time"
        paths.add(datePath)
        paths.add(timePath)
        dates.add(datePath, ts.date.toStr)
        times.add(timePath, ts.time.toStr)
      }

      curPath.removeAt(-1)
    }
  }

//////////////////////////////////////////////////////////////////////////
// Obj
//////////////////////////////////////////////////////////////////////////

  **
  ** Only used for unit tests
  **
  override Int hash() { id.hash }

  **
  ** Only used for unit tests
  **
  override Bool equals(Obj? that)
  {
    x := that as Rec
    if (x == null) return false
    return (
      (id    == x.id)    &&
      // path can come out in different order
      (paths.dup.sort == x.paths.dup.sort) &&
      (refs  == x.refs)  &&
      (strs  == x.strs)  &&
      (nums  == x.nums)  &&
      (units == x.units) &&
      (bools == x.bools) &&
      (uris  == x.uris)  &&
      (dates == x.dates) &&
      (times == x.times) &&
      (dateTimes == x.dateTimes)
    )
  }

  override Str toStr() {
    return Str[
      "Rec:",
      "    paths:     $paths",
      "    refs:      $refs",
      "    strs:      $strs",
      "    nums:      $nums",
      "    units:     $units",
      "    bools:     $bools",
      "    uris:      $uris",
      "    dates:     $dates",
      "    times:     $times",
      "    dateTimes: $dateTimes",
    ].join("\n")
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal const Str id
  internal const Str[] paths

  internal const Str:Str   refs
  internal const Str:Str   strs
  internal const Str:Float nums
  internal const Str:Str   units
  internal const Str:Bool  bools
  internal const Str:Str   uris
  internal const Str:Str   dates
  internal const Str:Str   times
  internal const Str:Int   dateTimes
}
