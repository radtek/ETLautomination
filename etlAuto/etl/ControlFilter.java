package etl;

import java.io.File;
import java.io.FilenameFilter;

class ControlFilter
  implements FilenameFilter
{
  static final int RCV_FLAG = 1;
  static final int MASTER_FLAG = 2;
  static final int MESSAGE_FLAG = 3;
  static final int LOG_FLAG = 4;
  private int rcv_master_flag;

  public ControlFilter(int mode)
  {
    this.rcv_master_flag = mode;
  }

  public boolean accept(File dir, String name) {
    if (name.length() <= 12)
      return false;
    if (this.rcv_master_flag == 1)
    {
      return name.startsWith("dir.");
    }

    if (this.rcv_master_flag == 2)
      if (name.endsWith(".DIR"))
        return true;
    else if (this.rcv_master_flag == 4)
      if (name.endsWith(".log"))
        return true;
    else if ((this.rcv_master_flag == 3) && 
      (name.endsWith(".msg"))) {
      return true;
    }
    return false;
  }
}
