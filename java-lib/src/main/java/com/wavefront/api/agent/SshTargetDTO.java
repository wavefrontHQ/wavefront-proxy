package com.wavefront.api.agent;

import com.google.common.base.Preconditions;

import java.util.UUID;

/**
 * Represents an SSH target to connect to.
 *
 * @author Clement Pang (clement@sunnylabs.com)
 */
public class SshTargetDTO {
  public UUID id;
  public String host;
  public int port = 22;
  public String hostKey;
  public String user;
  public String publicKey;

  public void validate(AgentConfiguration config) {
    Preconditions.checkNotNull(id, "id cannot be null for host");
    Preconditions.checkNotNull(host, "host cannot be null");
    Preconditions.checkState(port > 0, "port must be greater than 0");
    Preconditions.checkNotNull(publicKey, "publicKey cannot be null");
    if (user == null) user = config.defaultUsername;
    if (publicKey == null) publicKey = config.defaultPublicKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SshTargetDTO sshTargetDTO = (SshTargetDTO) o;

    if (port != sshTargetDTO.port) return false;
    if (host != null ? !host.equals(sshTargetDTO.host) : sshTargetDTO.host != null) return false;
    if (hostKey != null ? !hostKey.equals(sshTargetDTO.hostKey) : sshTargetDTO.hostKey != null)
      return false;
    if (!id.equals(sshTargetDTO.id)) return false;
    if (publicKey != null ? !publicKey.equals(sshTargetDTO.publicKey) : sshTargetDTO.publicKey != null)
      return false;
    if (user != null ? !user.equals(sshTargetDTO.user) : sshTargetDTO.user != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + (host != null ? host.hashCode() : 0);
    result = 31 * result + port;
    result = 31 * result + (hostKey != null ? hostKey.hashCode() : 0);
    result = 31 * result + (user != null ? user.hashCode() : 0);
    result = 31 * result + (publicKey != null ? publicKey.hashCode() : 0);
    return result;
  }
}
