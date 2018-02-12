import { MessageType } from "./MessageType"

export interface Message {
	type: MessageType
	info: any
}
