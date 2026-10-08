// Primitive native hosts only; React and its hooks are the real renderer runtime.
import { createElement } from 'react'
const host = (name: string) => (props: any) => createElement(name, props, props.children)
export const View = host('View'), Text = host('Text'), Image = host('Image'), Pressable = host('Pressable'), TextInput = host('TextInput'), ScrollView = host('ScrollView'), FlatList = host('FlatList'), Modal = host('Modal'), StatusBar = host('StatusBar'), SafeAreaView = host('SafeAreaView'), ActivityIndicator = host('ActivityIndicator'), Switch = host('Switch'), KeyboardAvoidingView = host('KeyboardAvoidingView'), RefreshControl = host('RefreshControl')
export const Platform = { OS: 'ios', Version: 18, select: (values: any) => values.ios ?? values.default }
export const StyleSheet = { create: (styles: any) => styles, flatten: (styles: any) => styles }
