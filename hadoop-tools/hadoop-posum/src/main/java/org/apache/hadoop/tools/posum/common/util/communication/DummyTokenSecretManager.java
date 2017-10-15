package org.apache.hadoop.tools.posum.common.util.communication;

import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.yarn.server.security.MasterKeyData;

import java.security.SecureRandom;

public class DummyTokenSecretManager extends SecretManager<DummyTokenIdentifier> {

  private int serialNo = new SecureRandom().nextInt();

  private MasterKeyData masterKey;

  public DummyTokenSecretManager() {
    // there should be a continuously updating key actually
    this.masterKey = new MasterKeyData(serialNo++, generateSecret());
  }

  @Override
  protected byte[] createPassword(DummyTokenIdentifier identifier) {
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  @Override
  public byte[] retrievePassword(DummyTokenIdentifier identifier) throws InvalidToken {
    return createPassword(identifier.getBytes(), masterKey.getSecretKey());
  }

  @Override
  public DummyTokenIdentifier createIdentifier() {
    return new DummyTokenIdentifier();
  }


}
