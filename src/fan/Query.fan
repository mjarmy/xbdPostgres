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
  new make(Filter f)
  {
    qb := QueryBuilder(f)
    this.sql =
      ["select * from rec",
       "where",
       "  ${qb.whereClause}"].join("\n") + ";"

    this.params = qb.whereParams
  }

  //////////////////////////////////////////////////////////////
  // Obj
  //////////////////////////////////////////////////////////////

  override Int hash() { throw UnsupportedErr() }

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

  private Void visitHas(FilterPath path)
  {
    whereClause.add("(rec.paths @> ?::jsonb)");
    whereParams.add("'{\"$path\"}'");
  }

  //////////////////////////////////////////////////////////////
  // Fields
  //////////////////////////////////////////////////////////////

  internal StrBuf whereClause
  internal Str[] whereParams
}


