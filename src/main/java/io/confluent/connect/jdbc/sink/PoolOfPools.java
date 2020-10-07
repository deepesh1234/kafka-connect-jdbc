package io.confluent.connect.jdbc.sink;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.Objects;

public class PoolOfPools {

  private static final Logger log = LoggerFactory.getLogger(PoolOfPools.class);
  private static final Map<Key, DataSource> DATASOURCES = Maps.newConcurrentMap();

  static class Key implements Comparable<Key> {
    final String url;
    final String username;
    final String password;

    Key(String url, String username, String password) {
      this.url = url;
      this.username = username;
      this.password = password;
    }

    @Override
    public int compareTo(Key that) {
      return ComparisonChain.start()
          .compare(this.url, that.url)
          .compare(this.username, that.username)
          .compare(this.password, that.password)
          .result();
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.url, this.username, this.password);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Key) {
        return 0 == compareTo((Key) obj);
      } else {
        return false;
      }
    }
  }

  static Key of(JdbcSinkConfig config) {
    return new Key(config.getString(JdbcSinkConfig.CONNECTION_URL),
        config.getString(JdbcSinkConfig.CONNECTION_USER),
        config.getPassword(JdbcSinkConfig.CONNECTION_PASSWORD).value());
  }

  public static DataSource get(final JdbcSinkConfig config) {
    Key key = of(config);

    return DATASOURCES.computeIfAbsent(key, k -> {
      String jdbcUrl = config.getString(JdbcSinkConfig.CONNECTION_URL);
      log.info("Creating connection pool for {}", jdbcUrl);
      HikariConfig hikariConfig = new HikariConfig();
      hikariConfig.setJdbcUrl(jdbcUrl);
      hikariConfig.setUsername(config.getString(JdbcSinkConfig.CONNECTION_USER));
      hikariConfig.setPassword(config.getPassword(JdbcSinkConfig.CONNECTION_PASSWORD).value());
      hikariConfig.setMaximumPoolSize(config.getInt(JdbcSinkConfig.MAX_POOL_SIZE_CONFIG));
      hikariConfig.setAutoCommit(false);

      return new HikariDataSource(hikariConfig);
    });
  }

}
