package org.apache.hadoop.tools.posum.common.util;

import org.apache.hadoop.tools.posum.client.data.Database;

public interface DatabaseProvider {

  Database getDatabase();
}
