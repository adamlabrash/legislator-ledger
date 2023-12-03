import { GeistSans } from 'geist/font/sans'
import './globals.css'
import { Suspense } from 'react';
import Nav from './nav';
import { Analytics } from '@vercel/analytics/react';
import Toast from './toast';


const defaultUrl = process.env.VERCEL_URL
  ? `https://${process.env.VERCEL_URL}`
  : 'http://localhost:3000'

export const metadata = {
  metadataBase: new URL(defaultUrl),
  title: 'Legislation Ledger',
  description: 'Non-partisan tracking of Canadian MP expenditures.',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en" className={GeistSans.className}>
      <body className="h-full">
        <Suspense>
          <Nav />
        </Suspense>
          {children}
        <Analytics />
        <Toast />
      </body>
    </html>
  )
}
