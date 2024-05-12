# xbdPostgres

## Notes

* See https://pganalyze.com/blog/gin-index
* database encoding must be UTF-8
* string: \u0000 is disallowed, as are Unicode escapes representing characters not available in the database encoding
* number: NaN and infinity values are disallowed
