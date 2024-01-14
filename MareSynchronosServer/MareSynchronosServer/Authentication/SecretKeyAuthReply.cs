namespace MareSynchronosServer.Authentication;

public record SecretKeyAuthReply(bool Success, string Uid, string Alias, bool TempBan, bool Permaban);
