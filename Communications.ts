import { Subject, Observable } from "rxjs"
import { PubSub } from "@daas/db-adapter"
import { Message } from "./Message"
import { MessageType } from "./MessageType"

export class Communications {
	private readonly requestId: string
	private readonly streamSubject: Subject<Message>
	private _close: () => Promise<void> = () =>
		Promise.reject(new Error("Comms not open"))

	public get adapterMessageStream() {
		return this.streamSubject.asObservable()
	}

	private get pubSubChannel() {
		return `core_adapter_communication_${this.requestId}`
	}

	public get close() {
		return this._close
	}

	private constructor(requestId: string) {
		this.requestId = requestId
		this.streamSubject = new Subject()
	}

	async sendMessage(type: MessageType, additionalInfo = {}) {
		await PubSub.notify(this.pubSubChannel, { type, info: additionalInfo })
	}

	waitForMessage(type: MessageType, timeout?: number): Promise<Message> {
		return (() => {
			const stream = this.adapterMessageStream.filter(it => it.type === type)

			if (timeout) {
				return Observable.merge(
					stream,
					Observable.throw(
						new Error(`Wait for message of type ${MessageType[type]} timed out`)
					)
						.materialize()
						.delay(timeout)
						.dematerialize()
				)
			} else {
				return stream
			}
		})()
			.take(1)
			.toPromise()
	}

	static async open(requestId: string) {
		const comms = new Communications(requestId)
		const unsubscribe = await PubSub.listen(
			comms.pubSubChannel,
			(msg: Message) => comms.streamSubject.next(msg)
		)
		comms._close = async () => {
			await unsubscribe()
			comms.streamSubject.complete()
		}
		return comms
	}
}
