//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** Query is a haystack Filter that has been translated to parameterized SQL
**
const class Query
{
  internal new make(Str sql, Str:Obj params)
  {
    this.sql = sql
    this.params = params
  }

  **
  ** Create a Query from a Filter
  **
  static new fromFilter(Haven haven, Filter f)
  {
    qb := QueryBuilder(haven, f)

    sql := StrBuf()

    if (qb.joins > 0)
    {
      // we have to order the records if we are doing a join
      sql.add("select rec.id, rec.brio from rec\n")

      for (i := 1; i <= qb.joins; i++)
      {
        prev := (i == 1) ? "rec" : "r${i-1}"
        sql.add("  inner join path_ref p$i on p${i}.source = ${prev}.id\n")
        sql.add("  inner join rec      r$i on r${i}.id     = p${i}.target\n")
      }
      sql.add("where\n")
      sql.add(qb.where).add("\n")

      // we have to order the records if we are doing a join
      sql.add("order by rec.id")
    }
    else
    {
      sql.add("select rec.brio from rec\n")
      sql.add("where\n")
      sql.add(qb.where)
    }

    return Query(sql.toStr, qb.params)
  }

//////////////////////////////////////////////////////////////////////////
// Obj
//////////////////////////////////////////////////////////////////////////

  override Int hash() { return sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query:\n$sql\nparams:$params" }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  **
  ** The parameterized SQL
  **
  const Str sql

  **
  ** The parameter name-to-value Map.
  **
  const Str:Obj params
}
