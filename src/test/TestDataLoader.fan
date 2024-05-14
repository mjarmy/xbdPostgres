//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** TestDataLoader insert the TestData into postgres
**
class TestDataLoader
{
  Void main()
  {
    TestDataLoader().load()
  }

  Void load()
  {
    storeHouse.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")

    n := 0
    td := TestData()
    td.recs.each |r,id|
    {
      storeHouse.insertRec(DbRec.fromDict(r))
      n++
    }
    echo("loaded $n recs")

    storeHouse.close()
  }

  private Storehouse storeHouse := Storehouse()
}
