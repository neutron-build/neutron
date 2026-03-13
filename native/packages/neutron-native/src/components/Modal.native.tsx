import { Modal as RNModal } from 'react-native'
import type { ModalProps } from '../types.js'

export function Modal({ children, ...props }: ModalProps) {
  return <RNModal {...props}>{children}</RNModal>
}
