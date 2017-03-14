package etl;

import java.util.Comparator;

class ControlFileCompare
  implements Comparator<String>
{
  public int compare(String a, String b)
  {
    String date1 = a.substring(a.length() - 8);
    String date2 = b.substring(b.length() - 8);
    return date1.compareTo(date2);
  }
}
