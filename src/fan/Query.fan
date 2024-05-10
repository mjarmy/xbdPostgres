//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

****************************************************************
** Query
****************************************************************

const class Query
{
  new make(Str sql, Str[] params)
  {
    this.sql = sql
    this.params = params
  }

  static new fromFilter(Filter f)
  {
    qb := QueryBuilder(f)

    return Query(
      ["select * from rec",
       "where",
       "  ${qb.whereClause}"].join("\n") + ";",
      qb.whereParams)
  }

  //////////////////////////////////////////////////////////////
  // Obj
  //////////////////////////////////////////////////////////////

  override Int hash() { return sql.hash.xor(params.hash) }

  override Bool equals(Obj? that)
  {
    x := that as Query
    if (x == null) return false
    return sql == x.sql && params == x.params
  }

  override Str toStr() { "Query:\n$sql\n$params" }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  const Str sql
  const Str[] params
}

****************************************************************
** QueryBuilder
****************************************************************

internal class QueryBuilder {

  new make(Filter f)
  {
    this.whereClause = StrBuf()
    this.whereParams = Str[,]

    visit(f)
  }

  private Void visit(Filter f)
  {
    if      (f.type == FilterType.has) visitHas(f.argA)
    //else if (f.type == FilterType.eq)  visitEq(f.argA, f.argB)
    //else if (f.type == FilterType.and) visitAnd(f.argA, f.argB)
    else throw Err("Encountered unknown FilterType ${f.type}")
  }

  private Void visitHas(FilterPath fp)
  {
    path := dotPath(fp)
    whereClause.add("(rec.paths @> ?::text[])");
    whereParams.add("{\"$path\"}");
  }

  private static Str dotPath(FilterPath fp)
  {
    sb := StrBuf()
    for (i := 0; i < fp.size; i++)
    {
      if (i > 0) sb.add(".")
      sb.add(fp.get(i))
    }
    return sb.toStr
  }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  internal StrBuf whereClause
  internal Str[] whereParams
}


