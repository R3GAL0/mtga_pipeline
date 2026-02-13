## Notes on how MTGA writes to the log



### Package structure

[UnityCrossThreadLogger] SOME PACKAGE METADATA
{PAYLOAD}

### Match starting/stopping signals

The match is starting signals/state switches (lines 725+)
I believe "Playing" is flipped during the waiting screen after a match is found and before the battlefield is loaded

[UnityCrossThreadLogger]STATE CHANGED {"old":"None","new":"ConnectedToMatchDoor"}
[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor","new":"ConnectedToMatchDoor_ConnectingToGRE"}
[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectingToGRE","new":"ConnectedToMatchDoor_ConnectedToGRE_Waiting"}
[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectedToGRE_Waiting","new":"Playing"}

Hard end of match signal

[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}

This event happens prior to match starting/stopping

MatchGameRoomStateChangedEvent

### Match result

The result of the match (who won/lost) 

[UnityCrossThreadLogger]2/12/2026 11:31:11 PM: Match to V7JT5YS7ANCWRHG35IZNV4PKOY: MatchGameRoomStateChangedEvent
{ "transactionId": "f192ba64-9c7b-4e34-96a2-1cf477e5e9e0", "requestId": 103, "timestamp": "1770957070891", "matchGameRoomStateChangedEvent": { "gameRoomInfo": { "gameRoomConfig": { "reservedPlayers": [ { "userId": "INPWZ5Y6HBC7ZBIBMGAIE66D6M", "playerName":"Bob", "systemSeatId": 1, "teamId": 1, "courseId": "Avatar_Basic_Rydia_FIN", "sessionId": "abf944a0-a239-49c5-9458-576bd6c6b978", "platformId": "iPhone", "eventId": "Play" }, { "userId": "V7JT5YS7ANCWRHG35IZNV4PKOY", "playerName":"R3GAL", "systemSeatId": 2, "teamId": 2, "courseId": "Avatar_Basic_JaceBeleren", "sessionId": "e2d82f81-8d85-434b-8830-fd57a4201b9e", "platformId": "Windows", "eventId": "Play" } ], "matchId": "2232b124-0796-47ba-bfb5-727f0d507814" }, "stateType": "MatchGameRoomStateType_MatchCompleted", "finalMatchResult": { "matchId": "2232b124-0796-47ba-bfb5-727f0d507814", "matchCompletedReason": "MatchCompletedReasonType_Success", "resultList": [ { "scope": "MatchScope_Game", "result": "ResultType_WinLoss", "winningTeamId": 1, "reason": "ResultReason_Game" }, { "scope": "MatchScope_Match", "result": "ResultType_WinLoss", "winningTeamId": 1, "reason": "ResultReason_Game" } ] } } } }



### Keys

This is my player id, it is consistent between sessions
V7JT5YS7ANCWRHG35IZNV4PKOY